require 'concurrent/collection/copy_on_notify_observer_set'
require 'concurrent/concern/observable'
require 'concurrent/synchronization/lockable_object'

module Concurrent
  class Agent < Synchronization::LockableObject

    # Observable handle returned by {Agent.defer}. It reports what happened to
    # the dispatches buffered by one deferred-send scope after the enclosing
    # action finished:
    #
    # * observers receive `:delivered` with the buffered dispatch for every
    #   dispatch accepted by its target,
    # * observers receive `:delivery_failed` with a {DeliveryFailure} when the
    #   target Agent had failed or the target executor rejected the dispatch,
    # * observers receive `:discarded` with the buffered dispatches when the
    #   enclosing action raised or its new value was rejected, in which case no
    #   delivery was ever attempted.
    #
    # Successful and failed deliveries are reported after the source state has
    # committed, in the exact order in which the dispatches were made, and each
    # dispatch is reported at most once.
    class DeferredSends < Synchronization::LockableObject
      include Concern::Observable

      def initialize
        super
        synchronize do
          @total     = 0
          @delivered = 0
          @failures  = []
          @discarded = 0
          @status    = :pending
          self.observers = Collection::CopyOnNotifyObserverSet.new
        end
      end

      # @return [Integer] the number of dispatches buffered by the scope
      def total_count
        synchronize { @total }
      end

      # @return [Symbol] `:pending`, `:delivered`, or `:discarded`
      def status
        synchronize { @status }
      end

      # @return [Integer] the number of dispatches accepted by their target
      def delivered_count
        synchronize { @delivered }
      end

      # @return [Integer] the number of dispatches attempted but rejected after
      #   the source state committed
      def failed_count
        synchronize { @failures.length }
      end

      # @return [Integer] the number of dispatches actually attempted after the
      #   source state committed (zero when the enclosing action failed)
      def sent_count
        synchronize { @delivered + @failures.length }
      end

      # @return [Integer] the number of buffered dispatches discarded without
      #   any delivery attempt
      def discarded_count
        synchronize { @discarded }
      end

      # @return [Array<DeliveryFailure>] the post-commit delivery failures
      def failures
        synchronize { @failures.dup }
      end

      # @return [Boolean] true while the enclosing action has not finished
      def pending?
        synchronize { @status == :pending }
      end

      # @return [Boolean] true when the buffered dispatches were forwarded
      def delivered?
        synchronize { @status == :delivered }
      end

      # @return [Boolean] true when the buffered dispatches were discarded
      def discarded?
        synchronize { @status == :discarded }
      end

      # @!visibility private
      def register_entry
        synchronize { @total += 1 }
      end

      # @!visibility private
      def notify_delivered(entry)
        synchronize do
          @delivered += 1
          @status     = :delivered
        end
        observers.notify_observers(:delivered, entry)
      end

      # @!visibility private
      def notify_delivery_failed(entry, reason)
        failure = DeliveryFailure.new(entry.target, reason, entry.executor)
        synchronize do
          @failures << failure
          @status = :delivered
        end
        observers.notify_observers(:delivery_failed, failure)
      end

      # @!visibility private
      def notify_discarded(entries)
        synchronize do
          @discarded += @total - @delivered - @failures.length
          @status = :discarded
        end
        observers.notify_observers(:discarded, entries)
      end
    end

    # Reports a dispatch which could not be forwarded after the source state
    # had already committed. The committed source state is never rolled back.
    class DeliveryFailure
      # @return [Agent] the target Agent which rejected the dispatch
      attr_reader :target

      # @return [Symbol] either `:failed_agent` or `:executor_rejected`
      attr_reader :reason

      # @return [Concurrent::ExecutorService] the target action executor
      attr_reader :executor

      # @!visibility private
      def initialize(target, reason, executor)
        @target   = target
        @reason   = reason
        @executor = executor
      end

      # @return [Boolean] true when the target Agent had failed
      def failed_target?
        reason == :failed_agent
      end

      # @return [Boolean] true when the target executor rejected the dispatch
      def executor_rejected?
        reason == :executor_rejected
      end
    end

    # One dispatch buffered by a {Agent.defer} scope.
    #
    # @!visibility private
    DeferredDispatch = Struct.new(:target, :action, :args, :executor, :caller,
                                  :scope)

    # Internal implementation of one `Agent.defer` scope.
    #
    # @!visibility private
    class DeferredScope
      attr_reader :handle

      def initialize(context)
        @context = context
        @handle  = DeferredSends.new
      end

      def capture_send(target, action, args, executor, caller)
        entry = DeferredDispatch.new(target, action, args, executor, caller, self)
        @context.record(entry)
        @handle.register_entry
        true
      end

      def self.deliver_entry(entry)
        job    = Job.new(entry.action, entry.args, entry.executor, entry.caller, true)
        result = entry.target.__send__(:deliver_deferred_job, job)

        if result == :ok
          entry.scope.handle.notify_delivered(entry)
        else
          entry.scope.handle.notify_delivery_failed(entry, result)
        end
      end
    end
    private_constant :DeferredDispatch, :DeferredScope
  end
end
