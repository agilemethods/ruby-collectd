module Collectd

  class PacketBuilder
    MAX_SIZE = 1024
    def initialize
      @head = nil
      @data = {}
    end

    def add(hostname, plugin, plugin_instance, type, type_instance, values)
      if @hostname
        return false if @hostname != hostname
      else
        @hostname = hostname
        @head = [Packet::Host.new(hostname),
                 Packet::Time.new(Time.now.to_i),
                 Packet::Interval.new(10)].join
      end
      pkt_plugin = Packet::Plugin.new(plugin)
      pkt_plugin_instance = Packet::PluginInstance.new(plugin_instance)
      pkt_type = Packet::Type.new(type)
      pkt_type_instance = Packet::TypeInstance.new(type_instance)
      add_size = [pkt_plugin, pkt_plugin_instance, pkt_type, pkt_type_instance, values].join.bytesize
      return false if size + add_size > MAX_SIZE
      @data[pkt_plugin] ||= {}
      @data[pkt_plugin][pkt_plugin_instance] ||= {}
      @data[pkt_plugin][pkt_plugin_instance][pkt_type] ||= {}
      @data[pkt_plugin][pkt_plugin_instance][pkt_type][pkt_type_instance] = values
      true
    end

    def size
      to_s.bytesize
    end

    def recurse_flatten(obj)
      if obj.is_a? Hash
        obj.map {|a,b| recurse_flatten(a) + recurse_flatten(b)}.join
      elsif obj.is_a? Array
        obj.join
      else
        obj.to_s
      end
    end

    def to_s
      @head ? @head + recurse_flatten(@data) : ''
    end
  end

end