begin
  require "em-mongo"
rescue LoadError => error
  raise "Missing EM-Synchrony dependency: gem install em-mongo"
end

module EM
  module Mongo

    def self.sync(response)
      if response.completed?
        response.succeeded? ? response.data : fail(response.error)
      else
        f = Fiber.current
        response.callback { |result| f.resume(result) }
        response.errback { |err| fail(err); f.resume }
        Fiber.yield
      end
    end

    def self.fail(error_array)
      raise error_array[0], error_array[1]
    end

    class Connection
      def initialize(host = DEFAULT_IP, port = DEFAULT_PORT, timeout = nil, opts = {})
        f = Fiber.current

        @em_connection = EMConnection.connect(host, port, timeout, opts)
        @db = {}

        # establish connection before returning
        EM.next_tick { f.resume }
        Fiber.yield
      end
    end

    class Database
      %w[get_last_error authenticate add_user].each do |method|
        class_eval %[
          alias :a#{method} :#{method}
          def #{method}(*args)
            EM::Mongo.sync a#{method}(*args)
          end
        ]
      end  
    end

    class Cursor
      %w[to_a explain count].each do |method|
        class_eval %[
          alias :a#{method} :#{method}
          def #{method}(*args)
            EM::Mongo.sync a#{method}(*args)
          end
        ]
      end  
    end

    class Collection

      %w[find_one find_and_modify map_reduce distinct group].each do |method|
        class_eval %[
          alias :a#{method} :#{method}
          def #{method}(*args)
            EM::Mongo.sync a#{method}(*args)
          end
        ]
      end  
      
      #em-mongo's safe_xxxx methods will pre-succeed
      #their deferrables before they return unless :safe => true,
      #so these methods will remain fire and forget and
      #return immediately unless the safe check is requested

      def ainsert(doc_or_docs, options = {})
        options[:safe] = false unless options[:safe] == true
        safe_insert(doc_or_docs, options)
      end     
      def insert(*args); EM::Mongo.sync ainsert(*args); end

      def aupdate(selector, document, options = {})
        options[:safe] = false unless options[:safe] == true
        safe_update(selector, document, options)
      end
      def update(*args); EM::Mongo.sync aupdate(*args); end

      def asave(doc, options={})
        options[:safe] = false unless options[:safe] == true
        safe_save(doc, options)
      end
      def save(*args); EM::Mongo.sync asave(*args); end
         
    end

  end
end
