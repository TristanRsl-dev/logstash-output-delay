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
  config :out, :validate => :string, :default => "stdout", :required => false
  config :hosts, :validate => :uri, :default => "//127.0.0.1", :required => false
  config :index, :validate => :string, :default => "logstash", :required => false
  config :document_id, :validate => :string, :required => false
  config :action, :validate => :string, :default => "index", :required => false
  config :doc_as_upsert, :validate => :boolean, :default => false, :required => false
  
  @events
  @output_plugin

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
	[[event, event]]
  end

  private
  def redirectMessageToPlugin(event_buffer)
	if @out == "stdout"
		event_buffer.each do |event|
			@output_plugin.multi_receive_encoded(encodeEvent(event))
			puts ""
		end
	elsif @out == "elasticsearch"
		@output_plugin.multi_receive(event_buffer)
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
	@output_plugin = nil
	chooseOutputPlugin()

	@output_plugin.register

	scheduler = Rufus::Scheduler.new

	scheduler_delay = @delay.to_s << "s"
	scheduler.every scheduler_delay do
		if @events.length != 0
			now = Time.new
			index = 0
			@events.detect {|event|
				index += 1
				event.time > now
			}
			redirectMessageToPlugin(@events.slice!(0, index).map {|e| e.event})
		end
	end
  end # def register

  public
  def receive(event)
	expended = OpenStruct.new
	expended.event = event
	expended.time = Time.new + @delay

	@events << expended

    return "Event received"
  end # def event
end # class LogStash::Outputs::Delay
