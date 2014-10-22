
require_relative 'core'

module STM
  # Public: Replace the named method with a wrapper that runs the body of the
  # method in a transaction. This can be used like:
  #
  #   transactional def foo(bar, baz)
  #     ...
  #   end
  #
  # or, given that this method returns the argument passed to it, like:
  #
  #   public transactional def foo(bar, baz)
  #     ...
  #   end
  #
  # to get the same effect as:
  #
  #   def foo(bar, baz)
  #     atomically do
  #       ...
  #     end
  #   end
  def transactional(method_name)
    raise NotImplementedError
  end
  
  
  def wait_until(seconds: nil, time: nil)
    STM.atomically do
      unless yield
        if STM.elapsed(seconds: seconds, time: time)
          raise Timeout
        else
          STM.try_later
        end
      end
    end
  end
  
  
  class ChangesOnlyPartialArguments
    def initialize(proc)
      @proc = proc
    end
    
    def according_to(predicate, &predicate_as_block)
      STM.changes_only(@proc, predicate || &predicate)
    end
  end
  
  
  def changes_only(proc=nil, according_to: nil, &proc_as_block)
    unless according_to
      return ChangesOnlyPartialArguments.new(proc || proc_as_block)
    end
    last = STM::TVar.new([false, nil])
    ->(value){
      has_run, last_value = last.get
      if !has_run || !according_to.call(last_value, value)
        last.set [true, value]
        proc.call(value)
      end
    }
  end
end

