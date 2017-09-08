# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/outputs/stdout"
require "logstash/outputs/elasticsearch"
require "rufus-scheduler"

# An delay output that does nothing.
class LogStash::Outputs::Delay < LogStash::Outputs::Base
    config_name "delay"
    concurrency :single

    config :delay, :validate => :number, :default => 5, :required => false
    config :batch_size, :validate => :number, :default => 10000, :required => false
    config :out, :validate => :string, :default => "stdout", :required => false
    config :hosts, :validate => :uri, :default => "//127.0.0.1", :required => false
    config :index, :validate => :string, :default => "logstash", :required => false
    config :document_id, :validate => :string, :required => false
    config :action, :validate => :string, :default => "index", :required => false
    config :doc_as_upsert, :validate => :boolean, :default => false, :required => false

    @events
    @times
    @output_plugin

    private
    def binarySearch(array, element)
        lower_bound = 0
        upper_bound = array.length - 1

        while lower_bound <= upper_bound
            middle_bound = lower_bound + ((upper_bound - lower_bound) / 2)
            if array[middle_bound] < element
                lower_bound = middle_bound + 1
            else
                upper_bound = middle_bound - 1
            end
        end

        return lower_bound
    end

    private
    def createElasticsearchConfig
        config = {
            "hosts" => @hosts,
            "index" => @index,
            "action" => @action,
            "document_id" => @document_id,
            "doc_as_upsert" => @doc_as_upsert
        }
        return config
    end

    private
    def encodeEvent(event)
        return [[event, event]]
    end

    private
    def redirectMessageToPlugin(event_buffer)
        if @out == "stdout"
            event_buffer.each do |event|
                @output_plugin.multi_receive_encoded(encodeEvent(event))
                puts ""
            end
        elsif @out == "elasticsearch"
            index = 0
            event_buffer_length = event_buffer.length
            while index < event_buffer_length do
                if index + @batch_size >= event_buffer_length
                    @output_plugin.multi_receive(event_buffer[index..event_buffer_length])
                else
                    @output_plugin.multi_receive(event_buffer[index, @batch_size])
                end
                index += @batch_size
            end
        else
            puts = "Choose between stdout or elasticsearch"
        end
        return nil
    end

    private
    def chooseOutputPlugin
        if @out == "stdout"
            @output_plugin = LogStash::Outputs::Stdout.new
        elsif @out == "elasticsearch"
            @output_plugin = LogStash::Outputs::ElasticSearch.new(createElasticsearchConfig())
        else
            puts "Choose between stdout or elasticsearch"
        end
        return nil
    end

    public
    def register
        @events = []
        @times = []
        @output_plugin = nil

        chooseOutputPlugin

        @output_plugin.register

        scheduler = Rufus::Scheduler.new

        scheduler_delay = @delay.to_s << "s"

        scheduler.every scheduler_delay do
            if @times.length != 0
                now = Time.new
                index = binarySearch(@times, now)
                redirectMessageToPlugin(@events.shift(index))
                @times.shift(index)
            end
        end
    end # def register

    public
    def receive(event)
        @events << event
        @times << Time.new + @delay

        return "Event received"
    end # def event
end # class LogStash::Outputs::Delay
