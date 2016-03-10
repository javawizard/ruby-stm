
require 'set'

module STM
  @last_transaction = 0
  GLOBAL_LOCK = Mutex.new

  # ...
  def self.last_transaction
    @last_transaction
  end

  def self.last_transaction= value
    @last_transaction = value
  end


  # Exception raised when a transaction needs to restart. This
  # happens when an attempt is made to read a variable that has been modified
  # since the transaction started. It also happens just after the transaction
  # has finished blocking in response to a TryLater.
  class Restart < Exception
  end

  # Exception Raised when a transaction should retry at some later
  # point, when at least one of the variables it accessed has been modified.
  # This happens when try_later() is called, and causes the toplevel
  # transaction to block until one of the variables accessed in this
  # transaction has been modified; the toplevel transaction then converts this
  # into a _Restart.
  class TryLater < Exception
  end


  # Set the current thread-local transaction to the specified
  # value.
  def self.set_current_transaction(transaction)
    # I'm not annoyed that Thread.current[] forces keys to be symbols instead
    # of e.g. singleton instances. Not at all...
    Thread.current[:a_really_long_and_unlikely_to_be_reused_stm_symbol] = transaction
  end

  # Get and return the current thread-local transaction, or return
  # nil if there isn't currently a transaction.
  def self.try_to_get_current_transaction
    Thread.current[:a_really_long_and_unlikely_to_be_reused_stm_symbol]
  end

  # Get and return the current thread-local transaction, or raise an
  # exception if there isn't currently a transaction.
  def self.get_current_transaction
    transaction = try_to_get_current_transaction
    # TODO: Add an exception class specifically for this
    raise "No current transaction" unless transaction
    transaction
  end

  # Run a block with the current transaction set to the specified
  # transaction, then restore the current transaction to what it was previously
  # at the end of the block.
  def self.with_current_transaction transaction
    former_transaction = try_to_get_current_transaction
    set_current_transaction transaction
    begin
      result = yield
    ensure
      set_current_transaction former_transaction
    end
    result
  end


  # ...
  #
  # This was actually a superclass in the Python version that had several
  # implementations, one of which would be selected for use depending on
  # certain conditions, solely on account of the fact that Python 2's
  # Condition.wait is implemented with a (throttled) busy loop when given a
  # timeout - the implementations used various tricks like selecting on a pipe
  # (which, thankfully, used native select() and its support for timeouts) or
  # starting a separate thread to notify the condition after the timeout
  # expired. Since YARV, at least, doesn't have those issues (it delegates
  # straight to pthread_cond_timedwait on POSIX platforms and... well, I don't
  # care enough about Windows to check what it does there), this class isn't
  # strictly necessary, but I'm using it to wrap up the mutex and condition
  # involved for the sake of convenience.
  class Waiter
    def initialize(resume_at)
      @mutex = Mutex.new
      @condition = ConditionVariable.new
      @resume_at = resume_at
    end

    def wait
      if @resume_at.nil?
        delay = nil
      else
        delay = @resume_at - Time.new.to_f
        if delay <= 0
          return
        end
      end
      # This is really stupid that we're required to have a mutex, especially
      # that broadcast doesn't even need one...
      @mutex.synchronize do
        @condition.wait(@mutex, delay)
      end
    end

    def notify
      @condition.broadcast
    end
  end


  # ...
  #
  # TODO: Make this a single thread that holds a priority queue of all
  # watchers waiting to be notified and notifies each of them as they expire
  class ElapsedWatcherNotifier
    def initialize(watcher, waiter)
      @watcher = watcher
      @waiter = waiter
      super() do
        @waiter.wait
        GLOBAL_LOCK.synchronize do
          if @watcher.notifier_thread != self
            return
          end
          self.watcher.notifier_thread = nil
          self.watcher.notifier_waiter = nil
        end
        atomically do
          STM.get_current_transaction.modified_set << self
        end
      end
    end

    # For compatibility with Transaction.modified_set
    def watchers
      [@watcher]
    end

    # For compatibility with Transaction.modified_set
    def check_clean
    end
  end


  # ...
  class Transaction
    attr_reader :read_set, :modified_set, :proposed_watchers, :resume_at, :watcher_resume_at

    def initialize
      @var_cache = {}
      @watchers_cache = {}
      @watched_vars_cache = {}
      @resume_watchers_at_cache = {}
      @read_set = Set.new
      @write_set = Set.new
      @modified_set = Set.new
      @watchers_changed_set = Set.new
      @watched_vars_changed_set = Set.new
      @proposed_watchers = []
      @resume_at = nil
      @watcher_resume_at = nil
    end

    def values_to_check_for_cleanliness
      @read_set | @write_set | @modified_set | @watchers_changed_set | @watched_vars_changed_set
    end

    def load_value(var)
      raise NotImplementedError
    end

    def load_watchers(var)
      raise NotImplementedError
    end

    def load_watched_vars(watcher)
      raise NotImplementedError
    end

    def run
      raise NotImplementedError
    end

    def get_value(var)
      # TODO: I read somewhere that exceptions are considerably slower than
      # the double lookup caused by this approach. Do some benchmarking to
      # figure out if that's really the case, and if it isn't, switch this to
      # use begin/@var_cache.fetch/rescue KeyError/end.
      unless @var_cache.key? var
        @read_set.add(var)
        @var_cache[var] = load_value(var)
      end
      @var_cache[var]
    end

    def set_value(var, value)
      @var_cache[var] = value
      @write_set.add(var)
      @modified_set.add(var)
    end

    def get_watchers(var)
      # TODO: Ditto
      unless @watchers_cache.key? var
        @watchers_changed_set.add(var)
        @watchers_cache[var] = load_watchers(var)
      end
      @watchers_cache[var]
    end

    def set_watchers(var, watchers)
      @watchers_changed_set.add(var)
      @watchers_cache[var] = watchers
    end

    def get_watched_vars(watcher)
      # TODO: Ditto
      unless @watched_vars_cache.key? watcher
        @watched_vars_changed_set.add(watcher)
        @watched_vars_cache[watcher] = load_watched_vars(watcher)
      end
      @watched_vars_cache[watcher]
    end

    def set_watched_vars(watcher, vars)
      @watched_vars_changed_set.add(watcher)
      @watched_vars_cache[watcher] = vars
    end

    def update_resume_at(resume_at)
      @resume_at = [resume_at, @resume_at || resume_at].min
      @parent.update_resume_at(resume_at) if @parent
    end

    def update_watcher_resume_at(resume_at)
      @watcher_resume_at = [resume_at, @watcher_resume_at || resume_at].min
      @parent.update_resume_at(resume_at) if @parent
    end

    def make_previously
      raise NotImplementedError
    end

    def base
      raise NotImplementedError
    end
  end


  # ...
  class BaseTransaction < Transaction
    attr_reader :start, :next_start_time

    def initialize(overall_start_time, current_start_time, start=nil)
      super()
      @overall_start_time = overall_start_time
      @current_start_time = current_start_time
      # TODO: Need to refresh my memory on why this isn't being recomputed
      # every re-run... Thinking the idea was that each subsequent run that was
      # due to a restart would function identically time-wise, but I'm no
      # longer sure that that's a good idea... (The transaction obviously won't
      # function identically to its previous run anyway as other vars it read
      # could have changed in the mean time.)
      @next_start_time = current_start_time
      @start = start
    end

    def base
      self
    end

    def load_value(var)
      GLOBAL_LOCK.synchronize do
        var.check_clean(self)
        var.real_value
      end
    end

    def load_watchers(var)
      GLOBAL_LOCK.synchronize do
        var.check_clean(self)
        Set.new(var.watchers)
      end
    end

    def load_watched_vars(watcher)
      GLOBAL_LOCK.synchronize do
        watcher.check_clean(self)
        Set.new(watcher.watched_vars)
      end
    end

    def run
      begin
        unless @start
          # TODO: See the same comment on the Python version - might possibly
          # be necessary in JRuby, but almost certainly not needed otherwise.
          GLOBAL_LOCK.synchronize do
            @start = STM.last_transaction
          end
        end

        result = yield

        commit
        result
      rescue TryLater
        try_later_setup
        try_later_block
      end
    end

    def commit
      watchers_to_run = Set.new
      @modified_set.each do |var|
        watchers_to_run.merge(get_watchers(var))
      end
      watchers_to_run.merge(@proposed_watchers)
      @proposed_watchers = []

      new_watchers_to_run = Set.new
      until watchers_to_run.empty?
        watchers_to_run.each do |watcher|
          formerly_watched_vars = get_watched_vars(watcher)
          watcher_transaction = NestedTransaction.new(self)
          result = STM.with_current_transaction watcher_transaction do
            watcher.run_watcher
          end
          newly_watched_vars = watcher_transaction.read_set
          set_watched_vars(watcher, newly_watched_vars)
          (formerly_watched_vars - newly_watched_vars).each do |formerly_watched_var|
            set_watchers(formerly_watched_var, get_watchers(formerly_watched_var) - [watcher])
          end
          (newly_watched_vars - formerly_watched_vars).each do |newly_watched_var|
            set_watchers(newly_watched_var, get_watchers(newly_watched_var) + [watcher])
          end
          @resume_watchers_at_cache[watcher] = watcher_transaction.watcher_resume_at

          callback_transaction = NestedTransaction.new(self)
          STM.with_current_transaction callback_transaction do
            watcher.run_callback(result)
          end
          callback_transaction.commit
          callback_transaction.modified_set.each do |var|
            new_watchers_to_run.merge(get_watchers(var))
          end
        end

        new_watchers_to_run.merge(@proposed_watchers)

        watchers_to_run = new_watchers_to_run
        new_watchers_to_run = Set.new
      end

      # Commit time!
      GLOBAL_LOCK.synchronize do
        values_to_check_for_cleanliness.each do |item|
          item.check_clean(self)
        end

        STM.last_transaction += 1
        modified = STM.last_transaction

        @write_set.each do |var|
          var.update_real_value(get_value(var))
          var.modified = modified
        end
        @watchers_changed_set.each do |var|
          var.watchers.replace(self.get_watchers(var))
          var.modified = modified
        end
        @watched_vars_changed_set.each do |watcher|
          watcher.watched_vars.replace(get_watched_vars(watcher))
          watcher.modified = modified
          if watcher.notifier_waiter
            watcher.notifier_thread = nil
            watcher.notifier_waiter.notify
          end
          if @resume_watchers_at_cache[watcher]
            watcher.notifier_waiter = Waiter.new(@resume_watchers_at_cache[watcher])
            watcher.notifier_thread = ElapsedWatcherNotifier.new(watcher, watcher.notifier_waiter)
          end
        end
      end
    end

    def try_later_setup
      GLOBAL_LOCK.synchronize do
        values_to_check_for_cleanliness.each do |item|
          item.check_clean(self)
        end
        w = Waiter.new(@resume_at)
        @read_set.each do |item|
          item.waiters.add(w)
        end
        @try_later_waiter = w
      end
    end

    def try_later_block
      w = @try_later_waiter

      w.wait()

      GLOBAL_LOCK.synchronize do
        @read_set.each do |item|
          item.waiters.delete(w)
        end
      end
      if @resume_at
        # TODO: I just used straight-up floats in the original Python version
        # - consider using Time objects instead
        @next_start_time = [@resume_at, Time.now.to_f].min
      else
        @next_start_time = @resume_at
      end

      raise Restart
    end

    def make_previously
      BaseTransaction.new(@overall_start_time, @current_start_time, @start)
    end
  end


  # ...
  class NestedTransaction < Transaction
    attr_reader :base

    def initialize(parent)
      super()
      @parent = parent
      @base = @parent.base
    end

    def load_value(var)
      @parent.get_value(var)
    end

    def load_watchers(var)
      @parent.get_watchers(var)
    end

    def load_watched_vars(watcher)
      @parent.get_watched_vars(watcher)
    end

    def run
      result = yield
      commit
      result
    end

    def commit
      @write_set.each do |var|
        @parent.set_value(var, @var_cache[var])
      end

      @parent.proposed_watchers.concat(@proposed_watchers)
      @watchers_changed_set.each do |var|
        @parent.set_watchers(var, get_watchers(var))
      end
      @watched_vars_changed_set.each do |watcher|
        @parent.set_watched_vars(watcher, get_watched_vars(watcher))
        @parent.resume_watchers_at_cache[watcher] = @resume_watchers_at_cache[watcher]
      end
    end

    def make_previously
      NestedTransaction.new(@parent)
    end
  end


  # A transactional variable.
  class TVar
    # ...
    attr_reader :real_value, :watchers, :waiters
    # ...
    attr_accessor :modified

    # Create a TVar with the specified initial value.
    def initialize(value=nil)
      @real_value = value
      @modified = 0
      @waiters = Set.new
      @watchers = Set.new
    end

    # Return the current value of this TVar.
    def get
      STM.possibly_atomically do
        STM.get_current_transaction.get_value(self)
      end
    end

    # Set the value of this TVar to the specified value.
    def set(value)
      STM.possibly_atomically do
        STM.get_current_transaction.set_value(self, value)
      end
      return
    end

    # ...
    def check_clean(transaction)
      raise Restart if @modified > transaction.start
    end

    # ...
    def update_real_value(value)
      @real_value = value
      @waiters.each do |w|
        w.notify
      end
    end
  end


  # ...
  class Watcher
    # ...
    attr_reader :watched_vars
    attr_accessor :modified, :notifier_waiter, :notifier_thread

    def initialize(proc, callback)
      @proc = proc
      @callback = callback
      @modified = 0
      # TODO: Find a WeakSet implementation to use here. This (most likely)
      # presents a memory leak as-is (since this causes a variable that's been
      # watched to implicitly hold a reference to all other variables watched
      # by the same watcher). Note: I have a really flimsy theory that
      # this might not actually present a memory leak (long story short: I
      # suspect that any change affecting the visibility of a TVar accessed by
      # a watcher would necessitate either another watched var's value changing
      # or the deallocation of all watched vars, but I'm far from certain that
      # that's actually true), so this might not actually be as big of a
      # problem as I'm making it out to be... Needs some more investigation.
      @watched_vars = Set.new
    end

    def check_clean(transaction)
      raise Restart if @modified > transaction.start
    end

    def run_watcher
      @proc.call
    end

    def run_callback(value)
      @callback.call(value)
    end
  end


  # TBD
  #
  # Note that bad things will happen if the block given to this method tries to
  # return or do other flow-interrupting things. A future version will allow
  # for these, but there are some kinks to be worked out.
  def self.atomically(&block)
    toplevel = !try_to_get_current_transaction
    if toplevel
      overall_start_time = Time.new.to_f
      current_start_time = overall_start_time
    end
    loop do
      if toplevel
        transaction = BaseTransaction.new(overall_start_time, current_start_time)
      else
        transaction = NestedTransaction.new(get_current_transaction)
      end
      with_current_transaction transaction do
        begin
          return transaction.run(&block)
        rescue Restart
          if toplevel
            current_start_time = transaction.next_start_time
            next
          else
            raise
          end
        end
      end
    end
  end


  def self.possibly_atomically(&block)
    if try_to_get_current_transaction
      block.call
    else
      atomically(&block)
    end
  end


  def self.try_later
    raise TryLater
  end


  def self.elapsed(seconds: nil, time: nil)
    if seconds && time
      raise ArgumentError("Only one of seconds and time can be specified")
    elsif !seconds && !time
      return false
    else
      transaction = get_current_transaction
      base = transaction.base
      if !!seconds
        time = base.overall_start_time + seconds
      end
      if base.start_time >= time
        return true
      else
        transaction.update_resume_at(time)
        if !seconds
          transaction.update_watcher_resume_at(time)
        end
        return false
      end
    end
  end


  def self.or_else(*procs)
    procs.each do |proc|
      begin
        return atomically(&proc)
      rescue TryLater
      end
    end
    try_later
  end


  def self.previously(toplevel = false)
    current = get_current_transaction
    if toplevel
      current = transaction.base
    end
    transaction = current.make_previously
    begin
      with_current_transaction transaction do
        yield
      end
    ensure
      if transaction.is_a? BaseTransaction
        current.read_set.merge(transaction.read_set)
        if transaction.resume_at
          current.update_resume_at(transaction.resume_at)
        end
        if transaction.watcher_resume_at
          current.update_watcher_resume_at(transaction.watcher_resume_at)
        end
      end
    end
  end


  # ...
  class WatchPartialArguments
    def initialize(proc)
      if proc.nil?
        raise ArgumentError("Either proc must be specified or a block must be given")
      end
      @function = proc
    end

    def callback(callback=nil, &callback_as_proc)
      STM.watch(@function, callback || callback_as_proc)
    end
  end


  def self.watch(proc=nil, callback=nil, &proc_as_block)
    if callback.nil?
      return WatchPartialArguments.new(proc || proc_as_block)
    end
    possibly_atomically do
      get_current_transaction.proposed_watchers << Watcher.new(proc || proc_as_block, callback)
    end
    return
  end
end



















