# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

# An delay output that does nothing.
class LogStash::Outputs::Delay < LogStash::Outputs::Base
  config_name "delay"
  concurrency :single
  
  config :delay, :validate => :number, :default => 5, :required => false
  
  @events

  public
  def register
	@events = []
  end # def register

  public
  def receive(message)
	event = OpenStruct.new
	event.message = message
	event.time = Time.new + @delay
	
	@events << event
	
	puts "[INFO][Delay] Event time: " + @events.at(-1).time.inspect
	puts "[INFO][Delay] Events tab size: " + @events.length.to_s
    return "Event received"
  end # def event
end # class LogStash::Outputs::Delay
