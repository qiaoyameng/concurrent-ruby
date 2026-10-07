require 'concurrent/executor/manual_timer_set'
require 'concurrent/executor/executor_service'
require 'concurrent/executor/immediate_executor'
require 'concurrent/scheduled_task'
require 'concurrent/timer_task'

module Concurrent

  RSpec.describe ManualTimerSet do

    # An executor which holds posted blocks until they are drained manually.
    class ManualQueueExecutor
      include ExecutorService

      def initialize
        @queue = []
      end

      def post(&block)
        @queue << block
      end

      def run_all
        @queue.shift.call until @queue.empty?
      end
    end

    let(:executor) { ImmediateExecutor.new }
    subject(:timer_set) { ManualTimerSet.new }

    def schedule(delay, opts = {}, &block)
      ScheduledTask.execute(delay, { timer_set: timer_set, executor: executor }.merge(opts), &block)
    end

    context 'construction' do

      it 'starts at virtual time zero by default' do
        expect(timer_set.now).to eq 0.0
      end

      it 'accepts an initial virtual time' do
        expect(ManualTimerSet.new(now: 42).now).to eq 42.0
      end

      it 'does not create any background threads' do
        expect {
          set = ManualTimerSet.new
          ScheduledTask.execute(1, timer_set: set, executor: executor) { nil }
          set.advance_to(10)
        }.not_to change { Thread.list.size }
      end
    end

    context '#advance_to' do

      it 'runs tasks due at or before the target time only' do
        ran = []
        schedule(5) { ran << :early }
        schedule(10) { ran << :late }

        timer_set.advance_to(5)
        expect(ran).to eq [:early]

        timer_set.advance_to(10)
        expect(ran).to eq [:early, :late]
      end

      it 'advances the virtual clock without real time passing' do
        before = Concurrent.monotonic_time
        timer_set.advance_to(1_000_000)
        expect(timer_set.now).to eq 1_000_000.0
        expect(Concurrent.monotonic_time - before).to be < 60
      end

      it 'runs tasks ordered by schedule time, then registration order' do
        ran = []
        schedule(3) { ran << :third }
        schedule(1) { ran << :first }
        schedule(1) { ran << :second }
        schedule(2) { ran << :between }

        timer_set.advance_to(3)
        expect(ran).to eq [:first, :second, :between, :third]
      end

      it 'returns the number of tasks executed' do
        schedule(1) { nil }
        schedule(1) { nil }
        schedule(5) { nil }
        expect(timer_set.advance_to(3)).to eq 2
      end

      it 'raises when asked to move time backwards' do
        timer_set.advance_to(10)
        expect { timer_set.advance_to(5) }.to raise_error(ArgumentError)
      end

      it 'schedules tasks using the virtual clock' do
        task = schedule(7) { nil }
        expect(task.schedule_time).to eq 7.0
      end

      it 'fulfills scheduled tasks through the task executor' do
        task = schedule(1) { 42 }
        expect(task).to be_pending
        timer_set.advance_to(1)
        expect(task).to be_fulfilled
        expect(task.value).to eq 42
      end

      it 'leaves the same event sequence whether time advances in segments or at once' do
        simulation = ->(advances) do
          set = ManualTimerSet.new
          events = []
          record = ->(name) { events << [name, set.now] }
          schedule_on = ->(delay, &block) do
            ScheduledTask.execute(delay, timer_set: set, executor: executor, &block)
          end
          schedule_on.call(1) { record.call(:a) }
          schedule_on.call(2.5) do
            record.call(:b)
            schedule_on.call(0.5) { record.call(:b_nested) }
          end
          schedule_on.call(2.5) { record.call(:c) }
          schedule_on.call(4) { record.call(:d) }
          advances.each { |time| set.advance_to(time) }
          events
        end

        segmented = simulation.call([1, 2, 3, 4, 5])
        at_once = simulation.call([5])
        expect(segmented).to eq at_once
        expect(at_once).to eq [[:a, 1.0], [:b, 2.5], [:c, 2.5], [:b_nested, 3.0], [:d, 4.0]]
      end
    end

    context 'reentrant scheduling and cancellation' do

      it 'runs tasks registered from within a task block during the same advance' do
        ran = []
        schedule(1) do
          ran << :outer
          schedule(0) { ran << :same_time }
          schedule(2) { ran << :later }
        end

        timer_set.advance_to(1)
        expect(ran).to eq [:outer, :same_time]

        timer_set.advance_to(3)
        expect(ran).to eq [:outer, :same_time, :later]
      end

      it 'cancels a queued task from within a task block' do
        ran = []
        other = nil
        schedule(1) do
          ran << :first
          expect(other.cancel).to be true
        end
        other = schedule(2) { ran << :cancelled }

        timer_set.advance_to(5)
        expect(ran).to eq [:first]
        expect(other).to be_rejected
        expect(other.reason).to be_a(CancelledOperationError)
      end

      it 'gives a defined result when cancelling a task selected but not yet called' do
        ran = []
        third = nil
        schedule(1) do
          ran << :first
          expect(third.cancel).to be true
        end
        schedule(1) { ran << :second }
        third = schedule(1) { ran << :third }

        expect(timer_set.advance_to(1)).to eq 2
        expect(ran).to eq [:first, :second]
        expect(third).to be_rejected
        expect(third.reason).to be_a(CancelledOperationError)
        expect(timer_set).to be_empty
      end

      it 'gives a defined result when cancelling a task dequeued but not yet called by its executor' do
        queue_executor = ManualQueueExecutor.new
        ran = false
        task = ScheduledTask.execute(1, timer_set: timer_set, executor: queue_executor) { ran = true }

        # the scheduler dequeues the task and posts it to the task's executor,
        # which holds it without calling it
        expect(timer_set.advance_to(1)).to eq 1
        expect(ran).to be false

        # same convention as TimerSet: the task no longer sits in the
        # scheduler queue, so the cancellation attempt reports false, but the
        # task is still transitioned and its block will never run
        expect(task.cancel).to be false
        expect(task).to be_rejected
        expect(task.reason).to be_a(CancelledOperationError)

        queue_executor.run_all
        expect(ran).to be false
      end

      it 'returns false when cancelling a task that already ran' do
        task = schedule(1) { nil }
        timer_set.advance_to(1)
        expect(task.cancel).to be false
      end
    end

    context 'step limit' do

      it 'bounds a single advance when tasks reschedule themselves at the current time' do
        count = 0
        reschedule = nil
        reschedule = -> do
          schedule(0) do
            count += 1
            reschedule.call
          end
        end
        reschedule.call

        expect(timer_set.advance_to(0, max_steps: 10)).to eq 10
        expect(count).to eq 10
        expect(timer_set).to be_due
      end

      it 'remains resumable after the step limit is reached' do
        count = 0
        reschedule = nil
        reschedule = -> do
          schedule(0) do
            count += 1
            reschedule.call if count < 25
          end
        end
        reschedule.call

        expect(timer_set.advance_to(0, max_steps: 10)).to eq 10
        expect(timer_set.advance_to(0, max_steps: 10)).to eq 10
        expect(timer_set.advance_to(0, max_steps: 10)).to eq 5
        expect(count).to eq 25
        expect(timer_set).not_to be_due
      end

      it 'uses the configured step limit by default' do
        set = ManualTimerSet.new(step_limit: 3)
        count = 0
        reschedule = nil
        reschedule = -> do
          ScheduledTask.execute(0, timer_set: set, executor: executor) do
            count += 1
            reschedule.call
          end
        end
        reschedule.call

        expect(set.advance_to(0)).to eq 3
        expect(count).to eq 3
      end
    end

    context 'with TimerTask' do

      it 'runs a fixed delay task on the virtual clock' do
        fires = []
        task = TimerTask.new(execution_interval: 5, timer_set: timer_set, executor: executor) do
          fires << timer_set.now
        end
        task.execute

        timer_set.advance_to(4)
        expect(fires).to be_empty

        timer_set.advance_to(5)
        timer_set.advance_to(10)
        timer_set.advance_to(16)
        expect(fires).to eq [5.0, 10.0, 15.0]
        task.shutdown
      end

      it 'runs immediately when :run_now is set and time advances' do
        fires = []
        task = TimerTask.new(execution_interval: 5, run_now: true,
                             timer_set: timer_set, executor: executor) { fires << timer_set.now }
        task.execute
        expect(fires).to be_empty

        timer_set.advance_to(0)
        expect(fires).to eq [0.0]
        task.shutdown
      end

      it 'measures fixed rate intervals from the start of each execution' do
        fires = []
        task = TimerTask.new(execution_interval: 5, interval_type: :fixed_rate,
                             timer_set: timer_set, executor: executor) do
          fires << timer_set.now
          timer_set.advance_to(timer_set.now + 2) # simulate 2 seconds of work
        end
        task.execute

        timer_set.advance_to(5)   # fires at 5, clock moves to 7, next at 5 + 5 = 10
        timer_set.advance_to(10)  # fires at 10, clock moves to 12, next at 10 + 5 = 15
        expect(fires).to eq [5.0, 10.0]
        expect(timer_set.now).to eq 12.0
        task.shutdown
      end

      it 'measures fixed delay intervals from the end of each execution' do
        fires = []
        task = TimerTask.new(execution_interval: 5, interval_type: :fixed_delay,
                             timer_set: timer_set, executor: executor) do
          fires << timer_set.now
          timer_set.advance_to(timer_set.now + 2) # simulate 2 seconds of work
        end
        task.execute

        timer_set.advance_to(5)   # fires at 5, clock moves to 7, next at 7 + 5 = 12
        timer_set.advance_to(12)  # fires at 12, clock moves to 14, next at 14 + 5 = 19
        expect(fires).to eq [5.0, 12.0]
        expect(timer_set.now).to eq 14.0
        task.shutdown
      end

      it 'stops firing after shutdown' do
        fires = []
        task = TimerTask.new(execution_interval: 1, timer_set: timer_set, executor: executor) do |t|
          fires << timer_set.now
          t.shutdown if fires.size >= 2
        end
        task.execute

        timer_set.advance_to(10)
        expect(fires).to eq [1.0, 2.0]
      end
    end

    context '#post' do

      it 'posts a task with a delay like TimerSet' do
        timer_set = ManualTimerSet.new(executor: executor)
        ran = false
        task = timer_set.post(3) { ran = true }
        expect(task).to be_a(ScheduledTask)
        timer_set.advance_to(3)
        expect(ran).to be true
      end

      it 'returns false after shutdown' do
        timer_set.shutdown
        expect(timer_set.post(1) { nil }).to be false
        expect(timer_set).not_to be_running
      end
    end

    context 'isolation' do

      it 'does not affect the global timer set' do
        schedule(1) { nil }
        timer_set.advance_to(1)

        expect(Concurrent.global_timer_set).to be_a(Concurrent::TimerSet)
        expect(Concurrent.global_timer_set).not_to be_a(ManualTimerSet)

        global_queue = Concurrent.global_timer_set.instance_variable_get(:@queue)
        expect { schedule(60) { nil } }.not_to change { global_queue.size }
      end

      it 'leaves tasks without an explicit timer set on the global timer set' do
        global_queue = Concurrent.global_timer_set.instance_variable_get(:@queue)
        task = nil
        expect {
          task = ScheduledTask.execute(60, executor: executor) { nil }
        }.to change { global_queue.size }.by(1)
        task.cancel
      end
    end
  end
end
