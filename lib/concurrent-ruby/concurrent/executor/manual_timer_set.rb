require 'concurrent/errors'
require 'concurrent/options'
require 'concurrent/scheduled_task'
require 'concurrent/executor/immediate_executor'

module Concurrent

  # A deterministic, manually driven replacement for {Concurrent::TimerSet},
  # intended primarily for tests. `ManualTimerSet` never creates any
  # background threads: instead of a real clock it keeps a *virtual*
  # monotonic time which only moves forward when explicitly told to via
  # `#advance_to` (or `#advance_by`). Due tasks are executed synchronously,
  # in order of their schedule time; tasks scheduled for the exact same
  # virtual time run in the order they were registered.
  #
  # It implements the same internal scheduling interface as
  # {Concurrent::TimerSet} (`post_task` / `remove_task`), so it can be
  # injected anywhere a timer set is accepted:
  #
  # * {Concurrent::ScheduledTask} through the `:timer_set` option
  # * {Concurrent::TimerTask} through the `:timer_set` option
  #
  # The executor on which the task bodies run can be chosen freely, as with
  # the real `TimerSet`. Passing an {Concurrent::ImmediateExecutor} (the
  # default for tasks posted through the timer set itself) makes every
  # callback run synchronously inside `#advance_to`, which keeps tests fully
  # deterministic.
  #
  # Callbacks are allowed to schedule or cancel tasks re-entrantly while an
  # advancement is in progress. A task re-scheduled for the *current* virtual
  # moment would run within the same `#advance_to` call; to prevent a
  # callback that keeps re-scheduling itself from looping forever, a single
  # `#advance_to` call executes at most `#max_steps` tasks. When the limit is
  # reached the remaining due tasks stay queued and the virtual time is left
  # at the last executed task, so a subsequent `#advance_to` call simply
  # continues where the previous one stopped.
  #
  # Cancelling a task which has already been dequeued but whose body has not
  # been called yet (possible when the task's executor runs asynchronously)
  # is well defined: `ScheduledTask#cancel` returns `true`, the task ends up
  # in the `:cancelled` state, and its block is never invoked.
  #
  # The global timer set ({Concurrent.global_timer_set}) is in no way
  # affected by this class; everything is strictly opt-in.
  #
  # @example
  #   scheduler = Concurrent::ManualTimerSet.new
  #   task = Concurrent::ScheduledTask.execute(5, timer_set: scheduler) { 42 }
  #   task.pending? #=> true
  #   scheduler.advance_to(4)   # nothing runs
  #   scheduler.advance_to(5)   # runs the task
  #   task.value #=> 42
  #
  # @see Concurrent::TimerSet
  class ManualTimerSet

    # Default limit of tasks executed by a single `#advance_to` call.
    DEFAULT_MAX_STEPS = 100

    # A single queued task, ordered by schedule time and registration order.
    #
    # @!visibility private
    class ScheduledEntry
      include Comparable

      attr_reader :schedule_time, :sequence_number, :task

      def initialize(schedule_time, sequence_number, task)
        @schedule_time   = schedule_time
        @sequence_number = sequence_number
        @task            = task
      end

      def <=>(other)
        (schedule_time <=> other.schedule_time).nonzero? ||
          (sequence_number <=> other.sequence_number)
      end
    end
    private_constant :ScheduledEntry

    # The maximum number of tasks a single `#advance_to` call will execute
    # before returning, even if more tasks are already due.
    attr_reader :max_steps

    # Create a new manual timer set.
    #
    # @param [Hash] opts the options used to configure the timer set
    # @option opts [Executor] :executor the default executor for tasks posted
    #   through `#post`; a new {Concurrent::ImmediateExecutor} when not given
    # @option opts [Numeric] :start_time (0.0) the initial virtual monotonic time
    # @option opts [Integer] :max_steps (DEFAULT_MAX_STEPS) the maximum number
    #   of tasks a single `#advance_to` call will execute
    def initialize(opts = {})
      @task_executor   = Options.executor_from_options(opts) || ImmediateExecutor.new
      @time            = opts.fetch(:start_time, 0.0).to_f
      @max_steps       = opts.fetch(:max_steps, DEFAULT_MAX_STEPS)
      @queue           = []
      @in_flight       = []
      @sequence_number = 0
      @shutdown        = false
      @mutex           = Mutex.new
    end

    # The current virtual monotonic time. Only moves forward, and only as a
    # result of `#advance_to` / `#advance_by`.
    #
    # @param [Symbol] unit the time unit to be returned, same units as
    #   {Concurrent.monotonic_time}
    #
    # @return [Float] the current virtual monotonic time
    def monotonic_time(unit = :float_second)
      time = synchronize { @time }
      case unit
      when :float_second then time
      when :float_millisecond then time * 1_000.0
      when :float_microsecond then time * 1_000_000.0
      when :second then time.to_i
      when :millisecond then (time * 1_000).to_i
      when :microsecond then (time * 1_000_000).to_i
      when :nanosecond then (time * 1_000_000_000).to_i
      else
        raise ArgumentError, "unexpected unit: #{unit.inspect}"
      end
    end

    # Advance the virtual time to the given point, executing every task
    # scheduled at or before that time (up to `max_steps` tasks). Tasks run
    # in order of schedule time, and tasks with equal schedule times run in
    # registration order. Tasks scheduled by the callbacks themselves for a
    # virtual time at or before the target time run within the same call.
    #
    # When more than `max_steps` tasks are due, only `max_steps` of them run;
    # the virtual time is left at the last executed task and the remaining
    # tasks stay queued, so calling `#advance_to` again with the same target
    # continues the advancement.
    #
    # @param [Numeric] time the virtual monotonic time to advance to
    # @param [Integer] max_steps the execution step limit for this call,
    #   defaults to `#max_steps`
    #
    # @return [Integer] the number of tasks executed by this call
    #
    # @raise [ArgumentError] when `time` is before the current virtual time
    def advance_to(time, max_steps: @max_steps)
      time = time.to_f
      synchronize do
        if time < @time
          raise ArgumentError, 'cannot advance to a time before the current virtual time'
        end
      end

      steps = 0
      loop do
        break if steps >= max_steps
        entry = synchronize do
          head = @queue.first
          @queue.shift if head && head.schedule_time <= time
        end
        break unless entry

        steps += 1
        synchronize { @time = entry.schedule_time if entry.schedule_time > @time }
        dispatch(entry.task)
      end

      synchronize do
        head = @queue.first
        # Only reach the target time when no due work remains; otherwise the
        # step limit was hit and the advancement can be continued later.
        @time = time if head.nil? || head.schedule_time > time
      end
      steps
    end

    # Advance the virtual time by the given number of seconds.
    #
    # @param [Numeric] seconds the number of seconds to advance by
    #
    # @return [Integer] the number of tasks executed by this call
    def advance_by(seconds)
      advance_to(monotonic_time + seconds)
    end

    # Post a task to be executed after the given delay (in virtual seconds).
    # The task runs on the executor given at construction.
    #
    # @param [Float] delay the number of (virtual) seconds to wait before
    #   executing the task
    # @param [Array<Object>] args the arguments passed to the task on execution
    #
    # @yield the task to be performed
    #
    # @return [Concurrent::ScheduledTask, false] IVar representing the task if
    #   the post is successful; false after shutdown
    #
    # @raise [ArgumentError] if no block is given
    def post(delay, *args, &task)
      raise ArgumentError.new('no block given') unless block_given?
      return false unless running?
      task = ScheduledTask.execute(delay,
                                   executor: @task_executor,
                                   args:     args,
                                   timer_set: self,
                                   &task)
      task.unscheduled? ? false : task
    end

    # Is the timer set still accepting new tasks?
    #
    # @return [Boolean] true when running, false after shutdown
    def running?
      synchronize { !@shutdown }
    end

    # Has the timer set been shut down?
    #
    # @return [Boolean] true after shutdown
    def shutdown?
      synchronize { @shutdown }
    end

    # Shut the timer set down, discarding all queued tasks. No threads are
    # involved, so this takes effect immediately.
    #
    # @return [true]
    def shutdown
      synchronize do
        @shutdown = true
        @queue.clear
      end
      true
    end
    alias_method :kill, :shutdown

    # The virtual monotonic time at which the next queued task is scheduled
    # to run.
    #
    # @return [Float, nil] the schedule time of the next task, or nil when
    #   nothing is queued
    def next_schedule_time
      synchronize { (head = @queue.first) && head.schedule_time }
    end

    # The number of tasks currently waiting in the queue.
    #
    # @return [Integer]
    def scheduled_count
      synchronize { @queue.size }
    end

    # Is no task currently queued?
    #
    # @return [Boolean]
    def empty?
      synchronize { @queue.empty? }
    end

    private

    def synchronize
      @mutex.synchronize { yield }
    end

    # Post the task to the internal queue.
    #
    # @note This is intended as a callback method from `ScheduledTask`
    #   only. It is not intended to be used directly.
    #
    # @!visibility private
    def post_task(task)
      synchronize do
        next false if @shutdown
        @sequence_number += 1
        @queue.push(ScheduledEntry.new(task.schedule_time, @sequence_number, task))
        @queue.sort!
        true
      end
    end

    # Remove the given task from the queue.
    #
    # A task which has already been dequeued but whose body has not been
    # called yet is still tracked, and is removed (and therefore never
    # invoked) as well.
    #
    # @note This is intended as a callback method from `ScheduledTask`
    #   only. It is not intended to be used directly.
    #
    # @return [Boolean] true when the task was still known to this timer set
    #   and has been removed, false when it was already gone
    #
    # @!visibility private
    def remove_task(task)
      synchronize do
        if @queue.reject! { |entry| entry.task.equal?(task) }
          true
        else
          !@in_flight.delete(task).nil?
        end
      end
    end

    # Hand the task to its executor. A cancelled task is never invoked, even
    # when it is cancelled after being dequeued.
    #
    # @!visibility private
    def dispatch(task)
      synchronize { @in_flight << task }
      begin
        task.executor.post do
          begin
            task.process_task unless task.cancelled?
          ensure
            synchronize { @in_flight.delete(task) }
          end
        end
      rescue RejectedExecutionError
        synchronize { @in_flight.delete(task) }
      end
    end
  end
end
