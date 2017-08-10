# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/outputs/stdout"

# An delay output that does nothing.
class LogStash::Outputs::Delay < LogStash::Outputs::Base
  config_name "delay"
  concurrency :single
  
  config :delay, :validate => :number, :default => 5, :required => false
  config :out, :validate => :string, :default => "stdout", :required => false
  
  @events
  @outputPlugin

  private
  def chooseOutputPlugin
	if @out == "stdout"
		@outputPlugin = LogStash::Outputs::Stdout.new
	elsif @out == "elasticsearch"
		puts "Not implemented yet"
	else
		puts "Choose between stdout or elasticsearch"
	end
  end
  
  public
  def register
	@events = []
	@outputPlugin = nil
	chooseOutputPlugin()

	Thread.new {
		loop do
			if @events.length != 0
				event = @events.at(0)
				if event.time < Time.new
					@outputPlugin.multi_receive_encoded([[event.message, event.message]])
					puts ""
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
