module STM
  module Helpers
    def self.included(including_class)
      including_class.extend(ClassMethods)
    end

    private def atomically(&block)
      STM.atomically(&block)
    end

    private def try_later
      STM.try_later
    end

    private def elapsed(seconds: nil, time: nil)
      STM.elapsed(seconds: seconds, time: time)
    end

    private def or_else(*procs)
      STM.or_else(*procs)
    end

    private def previously(toplevel = false, &block)
      STM.previously(toplevel, &block)
    end

    private def watch(proc = nil, callback = nil, &proc_as_block)
      STM.watch(proc, callback, &proc_as_block)
    end

    private def tvar(value = nil)
      STM::TVar.new(value)
    end

    module ClassMethods
      private def attr_transactional(*names)
        initializer = Module.new do
          define_method :initialize do |*args, &block|
            names.each do |name|
              instance_variable_set(:"@#{name}", STM::TVar.new)
            end

            super(*args, &block)
          end
        end

        prepend(initializer)

        names.each do |name|
          define_method name do
            instance_variable_get(:"@#{name}").get
          end

          define_method "#{name}=" do |value|
            instance_variable_get(:"@#{name}").set(value)
          end
        end

        names
      end

      private def transactional(method_name)

        transactionalizer = Module.new do
          Array(method_name).each do |name|
            define_method name do |*args, &block|
              STM.atomically do
                super(*args, &block)
              end
            end
          end
        end

        prepend(transactionalizer)

        method_name
      end
    end
  end
end