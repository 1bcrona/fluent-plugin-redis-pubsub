module Fluent
  class RedisPubsubInput < Input
    Plugin.register_input('redis_pubsub', self)

    attr_reader  :redis

    config_param :host, :string, default: 'localhost'
    config_param :port, :integer, default:  6379
    config_param :channel, :string
    config_param :tag, :string, default: nil
    config_param :id, :string, default:nil

    def initialize
      super
      require 'redis'
      require 'msgpack'
      require 'socket'
    end


    def ipv4_private
      Socket.ip_address_list.detect{|intf| intf.ipv4_private?}
    end

    def ipv4_public
      Socket.ip_address_list.detect{|intf| intf.ipv4? and !intf.ipv4_loopback? and !intf.ipv4_multicast? and !intf.ipv4_private?}
    end
  

    def configure(config)
      super
      @host    = config.has_key?('host')    ? config['host']         : 'localhost'
      @port    = config.has_key?('port')    ? config['port'].to_i    : 6379
      raise Fluent::ConfigError, "need channel" if not config.has_key?('channel') or config['channel'].empty?
      @channel = config['channel'].to_s
    end

    def start
      super
      @client_id = nil
      @ip = nil
      if config.has_key?('id')
         @client_id =config['id'].to_s
      else
         if ipv4_public.nil?
            @ip  = ipv4_private.ip_address
         else
            @ip = ipv4_public.ip_address
         end
            @client_id = "#{@ip}_#{@channel}"
      end
      
      @redis  = Redis.new(:host => @host, :port => @port ,:id=>@client_id,:thread_safe => true)
      @thread = Thread.new(&method(:run))
    end

    def run
      @redis.psubscribe @channel do |on|
        on.psubscribe do |channel, subscriptions|
          $log.info "Subscribed to ##{channel} (#{subscriptions} subscriptions)"
        end

        on.pmessage do |pattern, channel, msg|
          parsed = nil
          begin
            parsed = JSON.parse msg
          rescue JSON::ParserError => e
            $log.error e
          end
          Engine.emit @tag || channel, Engine.now, parsed || msg
        end
      end
    end

    def shutdown
      Thread.kill(@thread)
      @redis.quit
    end
  end
end
