require 'concurrent/atomic/atomic_reference'
require 'concurrent/collection/copy_on_notify_observer_set'
require 'concurrent/concern/observable'
require 'concurrent/synchronization/lockable_object'

# @!macro thread_safe_variable_comparison
#
#   ## Thread-safe Variable Classes
#
#   Each of the thread-safe variable classes is designed to solve a different
#   problem. In general:
#
#   * *{Concurrent::Agent}:* Shared, mutable variable providing independent,
#     uncoordinated, *asynchronous* change of individual values. Best used when
#     the value will undergo frequent, complex updates. Suitable when the result
#     of an update does not need to know immediately.
#   * *{Concurrent::Atom}:* Shared, mutable variable providing independent,
#     uncoordinated, *synchronous* change of individual values. Best used when
#     the value will undergo frequent reads but only occasional, though complex,
#     updates. Suitable when the result of an update must be known immediately.
#   * *{Concurrent::AtomicReference}:* A simple object reference that can be updated
#     atomically. Updates are synchronous but fast. Best used when updates a
#     simple set operations. Not suitable when updates are complex.
#     {Concurrent::AtomicBoolean} and {Concurrent::AtomicFixnum} are similar
#     but optimized for the given data type.
#   * *{Concurrent::Exchanger}:* Shared, stateless synchronization point. Used
#     when two or more threads need to exchange data. The threads will pair then
#     block on each other until the exchange is complete.
#   * *{Concurrent::MVar}:* Shared synchronization point. Used when one thread
#     must give a value to another, which must take the value.
#     The threads will block on each other until the exchange is complete.
#   * *{Concurrent::ThreadLocalVar}:* Shared, mutable, isolated variable which
#     holds a different value for each thread which has access. Often used as
#     an instance variable in objects which must maintain different state
#     for different threads.
#   * *{Concurrent::TVar}:* Shared, mutable variables which provide
#     *coordinated*, *synchronous*, change of *many* stated. Used when multiple
#     value must change together, in an all-or-nothing transaction.


module Concurrent

  # Atoms provide a way to manage shared, synchronous, independent state.
  #
  # An atom is initialized with an initial value and an optional validation
  # proc. At any time the value of the atom can be synchronously and safely
  # changed. If a validator is given at construction then any new value
  # will be checked against the validator and will be rejected if the
  # validator returns false or raises an exception.
  #
  # There are two ways to change the value of an atom: {#compare_and_set} and
  # {#swap}. The former will set the new value if and only if it validates and
  # the current value matches the new value. The latter will atomically set the
  # new value to the result of running the given block if and only if that
  # value validates.
  #
  # Every successful commit ({#swap}, {#compare_and_set}, {#reset}) produces a
  # new monotonically increasing {#version}, even when the old and new values
  # are equal. A rejected validator or a failed compare-and-set does not change
  # the version. Pairing the value with its version in an immutable
  # {Concurrent::Atom::Snapshot} (see {#snapshot}) allows change detection that
  # is immune to the ABA problem: observing the same value at two different
  # versions still proves that a change happened in between. {#wait_for_change}
  # blocks until a snapshot newer than a known version is available.
  #
  # ## Example
  #
  # ```
  # def next_fibonacci(set = nil)
  #   return [0, 1] if set.nil?
  #   set + [set[-2..-1].reduce{|sum,x| sum + x }]
  # end
  #
  # # create an atom with an initial value
  # atom = Concurrent::Atom.new(next_fibonacci)
  #
  # # send a few update requests
  # 5.times do
  #   atom.swap{|set| next_fibonacci(set) }
  # end
  #
  # # get the current value
  # atom.value #=> [0, 1, 1, 2, 3, 5, 8]
  # ```
  #
  # ## Waiting for changes
  #
  # {#snapshot} reads the value and its version together, atomically, and
  # {#wait_for_change} registers a waiter and blocks until the atom moves past
  # that version. Reading and registering happen under the same lock as the
  # commit, so no update at the boundary can be missed:
  #
  # ```
  # snapshot = atom.snapshot
  # loop do
  #   newer = atom.wait_for_change(snapshot.version, 5) # wait up to 5 seconds
  #   break if newer.nil?                                  # timeout, no change
  #   process(newer.value)
  #   snapshot = newer
  # end
  # ```
  #
  # ## Observation
  #
  # Atoms support observers through the {Concurrent::Observable} mixin module.
  # Notification of observers occurs every time the value of the Atom changes,
  # which means every successful commit, including commits which leave the value
  # equal. When notified the observer will receive three arguments: `time`,
  # `old_value`, and `new_value`. The `time` argument is the time at which the
  # value change occurred. The `old_value` is the value of the Atom when the
  # change began The `new_value` is the value to which the Atom was set when
  # the change completed. Note that `old_value` and `new_value` may be the same.
  # This is not an error. It simply means that the change operation returned
  # the same value.
  #
  # Observer callbacks are invoked outside of the atom's internal lock. An
  # observer may therefore mutate the atom (directly or indirectly) from within
  # its callback without risking a deadlock. The arguments delivered to an
  # observer are captured for that particular commit, so a delayed notification
  # of an older commit carries that commit's own values and can never overwrite
  # or undo a newer committed state.
  #
  # Unlike in Clojure, `Atom` cannot participate in {Concurrent::TVar} transactions.
  #
  # @!macro thread_safe_variable_comparison
  #
  # @see http://clojure.org/atoms Clojure Atoms
  # @see http://clojure.org/state Values and Change - Clojure's approach to Identity and State
  class Atom < Synchronization::LockableObject
    include Concern::Observable

    safe_initialization!
    public :synchronize

    # Immutable pairing of an atom value with the monotonically increasing
    # version at which that value was committed. Two snapshots with the same
    # value but different versions denote two distinct commits (an A→B→A
    # sequence), so comparing versions detects changes that comparing values
    # alone cannot.
    class Snapshot
      # @return [Object] the value of the atom when the snapshot was taken
      attr_reader :value

      # @return [Integer] the monotonically increasing version of the atom when
      #   the snapshot was taken
      attr_reader :version

      # @!visibility private
      def initialize(value, version)
        @value   = value
        @version = version
      end

      # @return [Array(Object, Integer)] the value and the version
      def to_a
        [value, version]
      end

      # @param [Object] other the object to compare with
      # @return [Boolean] true when `other` is a snapshot with the same version
      #   and value
      def ==(other)
        other.is_a?(Snapshot) &&
          version.equal?(other.version) &&
          value.equal?(other.value)
      end
      alias eql? ==

      # @return [Integer]
      def hash
        [Snapshot, version, value].hash
      end

      # @return [String]
      def inspect
        "#<#{self.class.name || 'Concurrent::Atom::Snapshot'} value=#{value.inspect} version=#{version}>"
      end
      alias to_s inspect
    end

    # @!method value
    #   The current value of the atom.
    #
    #   @return [Object] The current value.

    # Create a new atom with the given initial value.
    #
    # @param [Object] value The initial value
    # @param [Hash] opts The options used to configure the atom
    # @option opts [Proc] :validator (nil) Optional proc used to validate new
    #   values. It must accept one and only one argument which will be the
    #   intended new value. The validator will return true if the new value is
    #   acceptable else return false (preferably) or raise an exception.
    #
    # @!macro deref_options
    #
    # @raise [ArgumentError] if the validator is not a `Proc` (when given)
    def initialize(value, opts = {})
      super()
      @Validator     = opts.fetch(:validator, -> v { true })
      self.observers = Collection::CopyOnNotifyObserverSet.new
      synchronize do
        @value   = value
        @version = 0
      end
    end

    # The current value of the atom. Prefer {#snapshot} when the value is used
    # together with {#wait_for_change} or when changes must be detected.
    #
    # @return [Object] The current value.
    def value
      synchronize { @value }
    end
    alias_method :deref, :value

    # The current monotonically increasing version of the atom. The version
    # starts at zero and is incremented once by every successful commit.
    #
    # @return [Integer] The current version.
    def version
      synchronize { @version }
    end

    # Atomically reads the value together with the monotonically increasing
    # version. The returned snapshot is internally consistent: the value and the
    # version always describe the same committed state.
    #
    # @return [Snapshot] The current value and its version.
    def snapshot
      synchronize { Snapshot.new(@value, @version) }
    end

    # Atomically swaps the value of atom using the given block. The current
    # value will be passed to the block, as will any arguments passed as
    # arguments to the function. The new value will be validated against the
    # (optional) validator proc given at construction. If validation fails the
    # value will not be changed.
    #
    # Internally, {#swap} reads the current value, applies the block to it, and
    # attempts to compare-and-set it in. Since another thread may have changed
    # the value in the intervening time, it may have to retry, and does so in a
    # spin loop. The net effect is that the value will always be the result of
    # the application of the supplied block to a current value, atomically.
    # However, because the block might be called multiple times, it must be free
    # of side effects.
    #
    # @note The given block may be called multiple times, and thus should be free
    #   of side effects.
    #
    # @param [Object] args Zero or more arguments passed to the block.
    #
    # @yield [value, args] Calculates a new value for the atom based on the
    #   current value and any supplied arguments.
    # @yieldparam value [Object] The current value of the atom.
    # @yieldparam args [Object] All arguments passed to the function, in order.
    # @yieldreturn [Object] The intended new value of the atom.
    #
    # @return [Object] The final value of the atom after all operations and
    #   validations are complete.
    #
    # @raise [ArgumentError] When no block is given.
    def swap(*args)
      raise ArgumentError.new('no block given') unless block_given?

      loop do
        old_value = value
        new_value = yield(old_value, *args)
        begin
          break old_value unless valid?(new_value)
          break new_value if compare_and_set(old_value, new_value)
        rescue
          break old_value
        end
      end
    end

    # Atomically sets the value of atom to the new value if and only if the
    # current value of the atom is identical to the old value and the new
    # value successfully validates against the (optional) validator given
    # at construction. A successful set produces a new version even when the
    # old and new values are equal.
    #
    # @param [Object] old_value The expected current value.
    # @param [Object] new_value The intended new value.
    #
    # @return [Boolean] True if the value was committed else false.
    def compare_and_set(old_value, new_value)
      return false unless valid?(new_value)

      time      = Time.now
      committed = false

      synchronize do
        if @value.equal?(old_value)
          @value   = new_value
          @version += 1
          ns_broadcast
          committed = true
        end
      end

      observers.notify_observers(time, old_value, new_value) if committed
      committed
    end

    # Atomically sets the value of atom to the new value without regard for the
    # current value so long as the new value successfully validates against the
    # (optional) validator given at construction. A successful reset always
    # produces a new version, even when the new value equals the current value.
    #
    # @param [Object] new_value The intended new value.
    #
    # @return [Object] The final value of the atom after all operations and
    #   validations are complete.
    def reset(new_value)
      return value unless valid?(new_value)

      time      = Time.now
      old_value = synchronize do
        old_value = @value
        @value    = new_value
        @version += 1
        ns_broadcast
        old_value
      end

      observers.notify_observers(time, old_value, new_value)
      new_value
    end

    # Waits until the atom has been committed past the given version, then
    # returns the current {Snapshot}.
    #
    # If the atom is already past `version` the current snapshot is returned
    # immediately. Otherwise the waiter is registered atomically with the
    # commit and blocks until a new version is committed or `timeout` seconds
    # elapse. Registration and the stale-version check use the same lock as the
    # committing operation, so a commit happening at the boundary is either
    # observed before waiting (immediate return) or delivered to the registered
    # waiter; no update can fall between the two.
    #
    # A timeout only ends this wait; it never changes the atom. All waiters
    # registered for the same version observe the same commit, and intermediate
    # values which are committed and superseded while a waiter is blocked are
    # not preserved.
    #
    # @param [Integer] version A version previously obtained from a
    #   {Snapshot} (or {#version}); wait for any commit newer than it.
    # @param [Numeric, nil] timeout The maximum time in seconds to wait, or
    #   `nil` to wait indefinitely.
    #
    # @return [Snapshot, nil] The current snapshot when the atom moved past the
    #   given version, or `nil` when the timeout elapsed without a newer commit.
    def wait_for_change(version, timeout = nil)
      version = Integer(version)

      synchronize do
        changed = ns_wait_until(timeout) { @version > version }
        Snapshot.new(@value, @version) if changed
      end
    end

    private

    # Is the new value valid?
    #
    # @param [Object] new_value The intended new value.
    # @return [Boolean] false if the validator function returns false or raises
    #   an exception else true
    def valid?(new_value)
      @Validator.call(new_value)
    rescue
      false
    end
  end
end
