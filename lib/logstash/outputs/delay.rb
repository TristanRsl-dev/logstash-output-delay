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

    # This plugin is made for those who wants to apply a delay on each events.
    # It uses a rufus scheduler to send a batch of event when their delay are over.
    #
    # === How to use it
    #
    # .Exemple:
    #
    # A redirection of the event to stdout and a delay of one second.
    # [source,ruby]
    #     output {
    #         delay {
    #             delay => 1
    #         }
    #     }
    #
    # .Exemple:
    #
    # A redirection of the event to elasticsearch and a delay of two seconds.
    # [source,ruby]
    #     output {
    #         delay {
    #             delay => 2
    #             out => "elasticsearch"
    #             index => "logstash-delay"
    #         }
    #     }
    #
    # .Exemple:
    #
    # A redirection of the event to elasticsearch, a delay of three seconds,
    # a given document id, a batch size of 2000, a different host that the default one
    # and an update action.
    # [source,ruby]
    #
    #     output {
    #         delay {
    #             delay => 3
    #             batch_size => 2000
    #             out => "elasticsearch"
    #             hosts => "elasticsearch-on-my-server"
    #             index => "logstash-delay"
    #             document_id => "my_event_${number}"
    #             action => "update"
    #             doc_as_upsert => "true"
    #         }
    #     }

    # Set the delay to apply to all events. The value is in seconds.
    config :delay, :validate => :number, :default => 5, :required => false

    # Give a size to the batch that is used to keep events before sending them into Elasticsearch.
    config :batch_size, :validate => :number, :default => 10000, :required => false

    # Once the delay is over for an event, this one is send to another plugin that you can choose between the following:
    #
    # - "elasticsearch",
    # - "stdout".
    config :out, :validate => :string, :default => "stdout", :required => false

    # Sets the host(s) of the remote instance. If given an array it will load balance requests across the hosts specified in the `hosts` parameter.
    # Remember the `http` protocol uses the http://www.elastic.co/guide/en/elasticsearch/reference/current/modules-http.html#modules-http[http] address (eg. 9200, not 9300).
    #     `"127.0.0.1"`
    #     `["127.0.0.1:9200","127.0.0.2:9200"]`
    #     `["http://127.0.0.1"]`
    #     `["https://127.0.0.1:9200"]`
    #     `["https://127.0.0.1:9200/mypath"]` (If using a proxy on a subpath)
    # It is important to exclude http://www.elastic.co/guide/en/elasticsearch/reference/current/modules-node.html[dedicated master nodes] from the `hosts` list
    # to prevent LS from sending bulk requests to the master nodes.  So this parameter should only reference either data or client nodes in Elasticsearch.
    #
    # Any special characters present in the URLs here MUST be URL escaped! This means `#` should be put in as `%23` for instance.
    config :hosts, :validate => :uri, :default => "//127.0.0.1", :required => false


    # The index to write events to. This can be dynamic using the `%{foo}` syntax.
    # Indexes may not contain uppercase characters.
    # For weekly indexes ISO 8601 format is recommended, eg. logstash-%{+xxxx.ww}.
    # LS uses Joda to format the index pattern from event timestamp.
    # Joda formats are defined http://www.joda.org/joda-time/apidocs/org/joda/time/format/DateTimeFormat.html[here].
    config :index, :validate => :string, :default => "logstash", :required => false

    # The document ID for the index. Useful for overwriting existing entries in
    # Elasticsearch with the same ID.
    config :document_id, :validate => :string, :required => false

    # The Elasticsearch action to perform. Valid actions are:
    #
    # - index: indexes a document (an event from Logstash).
    # - delete: deletes a document by id (An id is required for this action)
    # - create: indexes a document, fails if a document by that id already exists in the index.
    # - update: updates a document by id. Update has a special case where you can upsert -- update a
    #   document if not already present. See the `upsert` option. NOTE: This does not work and is not supported
    #   in Elasticsearch 1.x. Please upgrade to ES 2.x or greater to use this feature with Logstash!
    # - A sprintf style string to change the action based on the content of the event. The value `%{[foo]}`
    #   would use the foo field for the action
    #
    # For more details on actions, check out the http://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html[Elasticsearch bulk API documentation]
    config :action, :validate => :string, :default => "index", :required => false

    # Enable `doc_as_upsert` for update mode.
    # Create a new document with source if `document_id` doesn't exist in Elasticsearch
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
