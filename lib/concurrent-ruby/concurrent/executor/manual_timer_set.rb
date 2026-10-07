require 'concurrent/scheduled_task'
require 'concurrent/errors'
require 'concurrent/options'

module Concurrent

  # A `TimerSet`-compatible scheduler driven by a virtual monotonic clock,
  # intended for tests. Unlike `TimerSet` it creates no background threads:
  # time only moves when `advance_to` (or `advance_by`) is called, and every
  # task due at that point runs on the calling thread, dispatched through each
  # task's own executor (use an `ImmediateExecutor` to run assertions inline).
  #
  # Advancing works like a discrete-event simulation: while the clock moves
  # toward the target time, each task runs with the clock positioned at its
  # own schedule time. Tasks due at the same virtual time run in registration
  # order. Advancing in several segments therefore leaves exactly the same
  # event sequence as a single advance to the same final time. Task blocks may
  # reentrantly schedule or cancel other tasks; cancelling a task that was
  # already dequeued by the scheduler but not yet called (for example because
  # its executor has not run it yet) follows the same convention as `TimerSet`:
  # `ScheduledTask#cancel` reports false since the task no longer sits in the
  # scheduler queue, yet the task is still transitioned and its block is
  # guaranteed to never run.
  #
  # A single advance performs at most `:step_limit` task executions (default
  # `DEFAULT_STEP_LIMIT`). This bounds pathological cases such as a task which
  # reschedules itself at the current time. When the limit is reached the
  # remaining due tasks stay queued and the next `advance_to` call resumes
  # where the previous one stopped.
  #
  # Because `ScheduledTask` and `TimerTask` ask their timer set for the current
  # time when it provides one, fixed-delay and fixed-rate `TimerTask`
  # calculations follow the virtual clock as well. The global timer set and
  # the real monotonic clock are never touched.
  #
  # @example
  #   timer_set = Concurrent::ManualTimerSet.new
  #   executor  = Concurrent::ImmediateExecutor.new
  #
  #   task = Concurrent::ScheduledTask.execute(10, timer_set: timer_set, executor: executor) { :boom }
  #   task.pending? #=> true
  #
  #   timer_set.advance_to(10)
  #   task.fulfilled? #=> true
  #   task.value      #=> :boom
  #
  # @see Concurrent::TimerSet
  class ManualTimerSet
    # Default maximum number of tasks executed by a single `advance_to` call.
    DEFAULT_STEP_LIMIT = 1000

    # The current virtual monotonic time.
    #
    # @return [Float] the current virtual time in seconds
    attr_reader :now

    # Create a new manual timer set.
    #
    # @param [Hash] opts the options used to configure the timer set
    # @option opts [Float] :now (0.0) the initial virtual time
    # @option opts [Integer] :step_limit (DEFAULT_STEP_LIMIT) the maximum
    #   number of tasks executed by a single `advance_to` call
    # @option opts [Executor] :executor the executor used by tasks posted
    #   through `#post` when no explicit executor is given
    def initialize(opts = {})
      @now            = opts.fetch(:now, 0.0).to_f
      @step_limit     = opts.fetch(:step_limit, DEFAULT_STEP_LIMIT)
      @task_executor  = Options.executor_from_options(opts) || Concurrent.global_io_executor
      @queue          = [] # entries are [schedule_time, registration_sequence, task]
      @sequence       = 0
      @running        = true
      @advancing      = false
      @mutex          = Mutex.new
    end

    # Is the timer set accepting new tasks?
    #
    # @return [Boolean] true when running, false after shutdown
    def running?
      @mutex.synchronize { @running }
    end

    # Post a task to be executed after the given delay (in virtual seconds).
    #
    # @param [Float] delay the number of virtual seconds to wait before executing the task
    # @param [Array<Object>] args the arguments passed to the task on execution
    #
    # @yield the task to be performed
    #
    # @return [Concurrent::ScheduledTask, false] IVar representing the task if the post
    #   is successful; false after shutdown
    def post(delay, *args, &task)
      raise ArgumentError.new('no block given') unless block_given?
      return false unless running?
      opts = { executor:  @task_executor,
               args:      args,
               timer_set: self }
      task = ScheduledTask.execute(delay, opts, &task) # may raise exception
      task.unscheduled? ? false : task
    end

    # Advance the virtual clock to the given time and run every task due at or
    # before it, in order of schedule time and registration. The clock is
    # positioned at each task's schedule time while that task runs, and rests
    # at the target time when the advance completes. Tasks scheduled from
    # within task blocks, including tasks scheduled at the current time, run
    # within the same advance unless the step limit is reached first. When the
    # step limit stops an advance early, the clock remains at the last
    # executed task's schedule time and the next advance resumes from there.
    #
    # Calling `advance_to` from within a task block only moves the virtual
    # clock; the tasks that become due are run by the outermost advance. This
    # can be used to simulate the passage of time while a task runs.
    #
    # @param [Float] time the virtual time to advance to; must not be in the past
    # @param [Integer] max_steps override of the `:step_limit` for this call
    #
    # @return [Integer] the number of tasks executed during this advance
    #
    # @raise [ArgumentError] when the given time is before the current virtual time
    def advance_to(time, max_steps: nil)
      time = time.to_f
      raise ArgumentError.new('cannot advance time backwards') if time < now
      max_steps ||= @step_limit
      if @advancing
        # Nested advance: only move the clock and extend the current advance;
        # the outermost advance runs the tasks that become due.
        @mutex.synchronize do
          @now = time
          @advance_target = time if time > @advance_target
        end
        return 0
      end
      @advancing = true
      @advance_target = time
      steps = 0
      limited = false
      begin
        loop do
          if steps >= max_steps
            limited = true
            break
          end
          entry = next_due_entry
          break unless entry
          @mutex.synchronize { @now = entry[0] if entry[0] > @now }
          steps += 1
          dispatch(entry[2])
        end
      ensure
        @advancing = false
        @mutex.synchronize { @now = @advance_target } if !limited && @advance_target > @now
      end
      steps
    end

    # Advance the virtual clock by the given delta.
    #
    # @param [Float] delta the number of virtual seconds to advance
    # @param [Integer] max_steps override of the `:step_limit` for this call
    #
    # @return [Integer] the number of tasks executed during this advance
    def advance_by(delta, max_steps: nil)
      advance_to(now + delta, max_steps: max_steps)
    end

    # Are there tasks due at or before the current virtual time? When true
    # after an `advance_to` call, the step limit was reached and another
    # advance (to the same or a later time) will resume execution.
    #
    # @return [Boolean] true when due tasks remain
    def due?
      @mutex.synchronize { @queue.any? { |entry| entry[0] <= @now } }
    end

    # The virtual time at which the next task is scheduled.
    #
    # @return [Float, nil] the earliest schedule time or nil when empty
    def next_schedule_time
      @mutex.synchronize { @queue.map(&:first).min }
    end

    # The number of tasks waiting in the queue.
    #
    # @return [Integer] the number of scheduled tasks
    def size
      @mutex.synchronize { @queue.size }
    end

    # Is the queue empty?
    #
    # @return [Boolean] true when no tasks are scheduled
    def empty?
      size.zero?
    end

    # Stop accepting new tasks and discard all queued tasks.
    #
    # @return [true]
    def shutdown
      @mutex.synchronize do
        @running = false
        @queue.clear
      end
      true
    end
    alias_method :kill, :shutdown

    private

    # The current virtual monotonic time. `ScheduledTask` and `TimerTask`
    # delegate to this method when their timer set provides one, so scheduled
    # times and fixed-rate calculations follow the virtual clock.
    #
    # @!visibility private
    def monotonic_time
      now
    end

    # Post the task to the internal queue.
    #
    # @note This is intended as a callback method from `ScheduledTask`
    #   only. It is not intended to be used directly.
    #
    # @!visibility private
    def post_task(task)
      @mutex.synchronize do
        return false unless @running
        @sequence += 1
        @queue << [task.schedule_time, @sequence, task]
      end
      true
    end

    # Remove the given task from the queue.
    #
    # @note This is intended as a callback method from `ScheduledTask`
    #   only. It is not intended to be used directly. Cancel a task
    #   by using the `ScheduledTask#cancel` method.
    #
    # @return [Boolean] true when the task was found and removed
    #
    # @!visibility private
    def remove_task(task)
      @mutex.synchronize do
        !!@queue.reject! { |entry| entry[2].equal?(task) }
      end
    end

    # Remove and return the earliest entry due at or before the current
    # advance target, ordered by schedule time and registration sequence.
    def next_due_entry
      @mutex.synchronize do
        entry = @queue.select { |candidate| candidate[0] <= @advance_target }
                      .min_by { |candidate| [candidate[0], candidate[1]] }
        @queue.delete(entry) if entry
        entry
      end
    end

    def dispatch(task)
      task.executor.post { task.process_task }
    rescue RejectedExecutionError
      # ignore and continue
    end
  end
end
