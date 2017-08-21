# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/outputs/stdout"
require "logstash/outputs/elasticsearch"

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
  @outputPlugin

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
  def redirectMessageToPlugin(message)
	if @out == "stdout"
		@outputPlugin.multi_receive_encoded([[message, message]])
		puts ""
	elsif @out == "elasticsearch"
		@outputPlugin.multi_receive([message])
	else
		puts = "Choose between stdout or elasticsearch"
	end
	return nil
  end

  private
  def chooseOutputPlugin
	if @out == "stdout"
		@outputPlugin = LogStash::Outputs::Stdout.new
	elsif @out == "elasticsearch"
		@outputPlugin = LogStash::Outputs::ElasticSearch.new(createElasticsearchConfig())
	else
		puts "Choose between stdout or elasticsearch"
	end
	return nil
  end
  
  public
  def register
	@events = []
	@outputPlugin = nil
	chooseOutputPlugin()

	@outputPlugin.register

	Thread.new {
		loop do
			if @events.length != 0
				event = @events.at(0)
				if event.time < Time.new
					redirectMessageToPlugin(event.message)
					@events.shift
				end
			end
		end
	}
  end # def register

  public
  def receive(message)
	event = OpenStruct.new
	event.message = message
	event.time = Time.new + @delay

	@events << event

    return "Event received"
  end # def event
end # class LogStash::Outputs::Delay
