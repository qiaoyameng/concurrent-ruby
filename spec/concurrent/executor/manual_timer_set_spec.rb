require 'concurrent/executor/manual_timer_set'
require 'concurrent/executor/immediate_executor'
require 'concurrent/scheduled_task'
require 'concurrent/timer_task'

module Concurrent

  RSpec.describe ManualTimerSet do

    let(:immediate) { Concurrent.global_immediate_executor }
    subject { ManualTimerSet.new }

    # An executor which accepts posts but only runs them when told to,
    # leaving tasks "dequeued but not yet called" from the timer's view.
    let(:queueing_executor) do
      Class.new(Concurrent::ImmediateExecutor) do
        def initialize
          super
          @posted = []
        end

        def post(*args, &task)
          raise ArgumentError.new('no block given') unless block_given?
          return false unless running?
          @posted << [task, args]
          true
        end

        def posted_count
          @posted.size
        end

        def run_next
          task, args = @posted.shift
          task.call(*args)
        end
      end.new
    end

    def schedule(delay, scheduler = subject, executor = immediate, &block)
      ScheduledTask.execute(delay, timer_set: scheduler, executor: executor, &block)
    end

    # A cancelled ScheduledTask ends up rejected with a
    # CancelledOperationError reason (see the ScheduledTask specs).
    def expect_cancelled(task)
      expect(task).to be_rejected
      expect(task.reason).to be_a(CancelledOperationError)
    end

    context 'construction' do
      it 'uses an ImmediateExecutor for posted tasks by default' do
        expect(ManualTimerSet.new.instance_variable_get(:@task_executor)).to be_a(ImmediateExecutor)
      end

      it 'uses the executor given at construction for posted tasks' do
        executor = ImmediateExecutor.new
        expect(ManualTimerSet.new(executor: executor).instance_variable_get(:@task_executor)).to eq executor
      end

      it 'starts at virtual time zero by default' do
        expect(subject.monotonic_time).to eq 0.0
      end

      it 'starts at the given start time' do
        expect(ManualTimerSet.new(start_time: 10).monotonic_time).to eq 10.0
      end

      it 'defaults max_steps to DEFAULT_MAX_STEPS' do
        expect(subject.max_steps).to eq ManualTimerSet::DEFAULT_MAX_STEPS
      end
    end

    context '#monotonic_time' do
      it 'only advances when told to' do
        expect(subject.monotonic_time).to eq 0.0
        subject.advance_to(2.5)
        expect(subject.monotonic_time).to eq 2.5
        subject.advance_by(1.5)
        expect(subject.monotonic_time).to eq 4.0
      end

      it 'supports the same units as Concurrent.monotonic_time' do
        subject.advance_to(1.5)
        expect(subject.monotonic_time(:float_millisecond)).to eq 1500.0
        expect(subject.monotonic_time(:millisecond)).to eq 1500
        expect(subject.monotonic_time(:second)).to eq 1
      end

      it 'refuses to move backwards' do
        subject.advance_to(2)
        expect { subject.advance_to(1) }.to raise_error(ArgumentError)
        expect(subject.monotonic_time).to eq 2.0
      end
    end

    context 'ScheduledTask integration' do
      it 'does not run tasks until the virtual time reaches their schedule time' do
        ran = false
        task = schedule(5) { ran = true }

        expect(subject.advance_to(4.999)).to eq 0
        expect(ran).to be false
        expect(task).to be_pending

        expect(subject.advance_to(5)).to eq 1
        expect(ran).to be true
        expect(task).to be_fulfilled
      end

      it 'computes schedule_time from the virtual clock' do
        scheduler = ManualTimerSet.new(start_time: 100)
        task = schedule(5, scheduler) { nil }
        expect(task.schedule_time).to eq 105.0
      end

      it 'fulfills the task with the block result' do
        task = schedule(1) { 42 }
        subject.advance_to(1)
        expect(task.value).to eq 42
      end

      it 'supports reschedule against the virtual clock' do
        ran = false
        task = schedule(5) { ran = true }
        task.reschedule(10)
        expect(task.schedule_time).to eq 10.0
        subject.advance_to(5)
        expect(ran).to be false
        subject.advance_to(10)
        expect(ran).to be true
      end

      it 'runs tasks in order of schedule time, then registration order' do
        order = []
        schedule(3) { order << :third }
        schedule(1) { order << :first }
        schedule(1) { order << :second }
        schedule(2) { order << :between }
        schedule(3) { order << :fourth }

        subject.advance_to(3)
        expect(order).to eq [:first, :second, :between, :third, :fourth]
      end
    end

    context '#post' do
      it 'posts a task with arguments to be run after the delay' do
        received = nil
        task = subject.post(2, 1, 2) { |a, b| received = a + b }
        expect(task).to be_a(ScheduledTask)
        subject.advance_to(2)
        expect(received).to eq 3
      end

      it 'raises an exception when no block is given' do
        expect { subject.post(1) }.to raise_error(ArgumentError)
      end

      it 'returns false after shutdown' do
        subject.shutdown
        expect(subject.post(1) { nil }).to be false
      end
    end

    context 're-entrant scheduling' do
      it 'runs tasks scheduled by callbacks for the current moment within the same advancement' do
        events = []
        schedule(1) do
          events << :first
          schedule(0) { events << :nested }
          schedule(5) { events << :later }
        end

        expect(subject.advance_to(1)).to eq 2
        expect(events).to eq [:first, :nested]
        expect(subject.monotonic_time).to eq 1.0

        expect(subject.advance_to(6)).to eq 1
        expect(events).to eq [:first, :nested, :later]
      end
    end

    context 'cancellation' do
      it 'cancels a queued task from within another callback' do
        events = []
        victim = nil
        schedule(1) { events << [:cancel, victim.cancel] }
        victim = schedule(1) { events << :victim }

        subject.advance_to(1)
        expect(events).to eq [[:cancel, true]]
        expect_cancelled(victim)
      end

      it 'gives a well-defined result when cancelling a dequeued but not yet called task' do
        ran = false
        task = schedule(1, subject, queueing_executor) { ran = true }

        expect(subject.advance_to(1)).to eq 1
        expect(queueing_executor.posted_count).to eq 1
        expect(ran).to be false

        expect(task.cancel).to be true
        expect_cancelled(task)

        queueing_executor.run_next
        expect(ran).to be false
        expect_cancelled(task)
      end

      it 'returns false when cancelling a task unknown to the timer set' do
        task = schedule(1) { nil }
        subject.advance_to(1)
        expect(task.cancel).to be false
      end
    end

    context 'step limit' do
      it 'bounds a single advancement and remains continuable' do
        scheduler = ManualTimerSet.new(max_steps: 3)
        runs = 0
        reschedule = nil
        reschedule = ->(*) do
          runs += 1
          ScheduledTask.execute(0, timer_set: scheduler, executor: immediate, &reschedule)
        end
        ScheduledTask.execute(0, timer_set: scheduler, executor: immediate, &reschedule)

        expect(scheduler.advance_to(0)).to eq 3
        expect(runs).to eq 3
        expect(scheduler.monotonic_time).to eq 0.0
        expect(scheduler).not_to be_empty
        expect(scheduler.next_schedule_time).to eq 0.0

        expect(scheduler.advance_to(0)).to eq 3
        expect(runs).to eq 6

        expect(scheduler.advance_to(0, max_steps: 10)).to eq 10
        expect(runs).to eq 16
      end

      it 'reaches the target time once no due work remains' do
        scheduler = ManualTimerSet.new(max_steps: 3)
        schedule(1, scheduler) { nil }
        expect(scheduler.advance_to(5)).to eq 1
        expect(scheduler.monotonic_time).to eq 5.0
      end
    end

    context 'TimerTask integration' do
      it 'runs a fixed delay task with the defined cadence' do
        events = []
        timer = TimerTask.new(execution_interval: 2,
                              interval_type: TimerTask::FIXED_DELAY,
                              timer_set: subject,
                              executor: immediate) { events << subject.monotonic_time }
        timer.execute

        subject.advance_to(2)
        expect(events).to eq [2.0]
        subject.advance_to(4)
        expect(events).to eq [2.0, 4.0]
        subject.advance_to(7)
        expect(events).to eq [2.0, 4.0, 6.0]
        timer.shutdown
      end

      it 'runs a fixed rate task with the defined cadence' do
        events = []
        timer = TimerTask.new(execution_interval: 2,
                              interval_type: TimerTask::FIXED_RATE,
                              timer_set: subject,
                              executor: immediate) { events << subject.monotonic_time }
        timer.execute

        subject.advance_to(5)
        expect(events).to eq [2.0, 4.0]
        subject.advance_to(6)
        expect(events).to eq [2.0, 4.0, 6.0]
        timer.shutdown
      end

      it 'exposes the last value and stops after shutdown' do
        events = []
        timer = TimerTask.new(execution_interval: 1,
                              timer_set: subject,
                              executor: immediate) { events << subject.monotonic_time; events.size }
        timer.execute

        subject.advance_to(2)
        expect(events).to eq [1.0, 2.0]
        expect(timer.value).to eq 2

        timer.shutdown
        subject.advance_to(10)
        expect(events).to eq [1.0, 2.0]
      end

      it 'honors run_now at the current virtual time' do
        events = []
        timer = TimerTask.new(run_now: true,
                              execution_interval: 5,
                              timer_set: subject,
                              executor: immediate) { events << subject.monotonic_time }
        timer.execute

        expect(events).to eq []
        subject.advance_to(0)
        expect(events).to eq [0.0]
        subject.advance_to(5)
        expect(events).to eq [0.0, 5.0]
        timer.shutdown
      end
    end

    context 'determinism' do
      # Three tasks at the same virtual time; the first callback re-enters
      # (schedules another task for the current moment) and cancels the
      # third task. Returns the recorded event sequence.
      def run_scenario
        scheduler = ManualTimerSet.new
        events = []
        tasks = {}
        tasks[:a] = schedule(5, scheduler) do
          events << :a
          tasks[:d] = schedule(0, scheduler) { events << :d }
          events << [:cancel_c, tasks[:c].cancel]
        end
        tasks[:b] = schedule(5, scheduler) { events << :b }
        tasks[:c] = schedule(5, scheduler) { events << :c }
        [scheduler, events, tasks]
      end

      it 'runs same-time tasks in registration order with re-entrant scheduling and cancellation' do
        scheduler, events, tasks = run_scenario
        scheduler.advance_to(5)
        expect(events).to eq [:a, [:cancel_c, true], :b, :d]
        expect_cancelled(tasks[:c])
        expect(scheduler).to be_empty
      end

      it 'leaves the same event sequence whether advancing in segments or in one step' do
        one_shot_scheduler, one_shot_events, = run_scenario
        one_shot_scheduler.advance_to(5)

        segmented_scheduler, segmented_events, = run_scenario
        segmented_scheduler.advance_to(2)
        segmented_scheduler.advance_to(4.5)
        segmented_scheduler.advance_to(5)

        expect(segmented_events).to eq one_shot_events
        expect(segmented_scheduler.monotonic_time).to eq one_shot_scheduler.monotonic_time
      end
    end

    context 'shutdown' do
      it 'discards queued tasks and stops accepting new ones' do
        ran = false
        schedule(1) { ran = true }
        expect(subject).to be_running

        subject.shutdown
        expect(subject).to be_shutdown
        expect(subject).not_to be_running
        expect(subject).to be_empty

        expect(subject.advance_to(2)).to eq 0
        expect(ran).to be false
      end
    end

    context 'global timer set' do
      it 'is not affected by the manual timer set' do
        expect(Concurrent.global_timer_set).to be_a(TimerSet)

        subject # force creation of the manual timer set
        task = ScheduledTask.execute(60) { nil }
        begin
          expect(task.schedule_time).to be_within(1).of(Concurrent.monotonic_time + 60)
        ensure
          task.cancel
        end
      end
    end
  end
end
