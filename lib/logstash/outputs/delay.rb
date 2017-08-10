# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "logstash/outputs/stdout"

# An delay output that does nothing.
class LogStash::Outputs::Delay < LogStash::Outputs::Base
  config_name "delay"
  concurrency :single
  
  config :delay, :validate => :number, :default => 5, :required => false
  
  @events
  @outputPlugin

  public
  def register
	@events = []
	@outputPlugin = LogStash::Outputs::Stdout.new

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
