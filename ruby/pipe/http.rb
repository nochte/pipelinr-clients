require 'httparty'
# require_relative '../entities/message'
require_relative '../lib/retry'

def interpret_http_responses(jsary) 
  jsary.map{|js| interpret_http_response(js)}
end

def interpret_http_response(js) 
  case js['status']
  when nil 
    raise "error sending. unknown status}"
  when 0...200
    raise "error sending. #{js['topic']}: #{js['text']}"
  when 300..999
    raise "error sending. #{js['topic']}: #{js['text']}"
  end

  # return the id
  js['text']
end

class HTTPPipe 
  attr_reader :running
  def initialize(url: "http://pipelinr.dev", apikey:, pipe:)
    raise 'apikey and pipe required' if apikey.nil? || pipe.nil?
    @url = url
    @apikey = apikey
    @pipe = pipe
    @receive_options = {
      auto_ack: false,
      block: false,
      count: 1,
      timeout: 0,
      redelivery_timeout: 0
    }
    @messages = Queue.new
    @mutex = Mutex.new
    @running = false
  end

  def length
    @messages.length
  end

  def receive_options=(auto_ack: nil, block: nil, count: nil, timeout: nil, redelivery_timeout: nil)
    @receive_options[:auto_ack] = auto_ack unless auto_ack.nil?
    @receive_options[:block] = block unless block.nil?
    @receive_options[:count] = count unless count.nil?
    @receive_options[:timeout] = timeout unless timeout.nil?
    @receive_options[:redelivery_timeout] = redelivery_timeout unless redelivery_timeout.nil?
  end

  def send(payload: nil, route: [])
    if payload.nil? || route.nil? || route.length == 0 
      raise "payload must be present and route must be an array of strings"
    end
    response = HTTParty.post(
      "#{@url}/api/2/pipes",
        body: {Route: route, Payload: payload}.to_json,
        headers: {
          "Content-Type": 'application/json',
          Authorization: "api #{@apikey}"
        }
    )
    interpret_http_response(response.parsed_response)
  end

  def ack(id)
    response = HTTParty.put(
      "#{@url}/api/2/message/#{id}/ack/#{@pipe}",
      headers: {
        "Content-Type": 'application/json',
        Authorization: "api #{@apikey}"
      }
    )
    interpret_http_response(response.parsed_response)
  end

  def complete(id)
    response = HTTParty.put(
      "#{@url}/api/2/message/#{id}/complete/#{@pipe}",
      headers: {
        "Content-Type": 'application/json',
        Authorization: "api #{@apikey}"
      }
    )
    interpret_http_response(response.parsed_response)
  end

  def log(id, code, message)
    raise 'id, code, message all required' if id.nil? || code.nil? || message.nil?

    response = HTTParty.patch(
      "#{@url}/api/2/message/#{id}/log/#{@pipe}",
      body: {Code: code, Message: message}.to_json,
      headers: {
        "Content-Type": 'application/json',
        Authorization: "api #{@apikey}"
      }
    )
    interpret_http_response(response.parsed_response)
  end

  def add_steps(id, steps)
    raise 'id, steps required' if id.nil? || steps.nil? || steps.length == 0

    response = HTTParty.patch(
      "#{@url}/api/2/message/#{id}/route",
      body: {After: @pipe, NewSteps: steps}.to_json,
      headers: {
        "Content-Type": 'application/json',
        Authorization: "api #{@apikey}"
      }
    )
    interpret_http_response(response.parsed_response)
  end

  # decorations is an array of {Key: string, Value: string}
  def decorate(id, decorations)
    raise 'id, decorations required' if id.nil? || decorations.nil? || decorations.length == 0

    response = HTTParty.patch(
      "#{@url}/api/2/message/#{id}/decorations",
      body: {Decorations: decorations}.to_json,
      headers: {
        "Content-Type": 'application/json',
        Authorization: "api #{@apikey}"
      }
    )

    interpret_http_responses(response.parsed_response)
  end

  # start returns a thread that can be waited on
  def start
    raise 'already running' if @running
    @running = true
    Thread.new do
      while @running do
        if @messages.length >= @receive_options[:count]
          sleep 0.25
          next
        end
        fetch_with_backoff.each do |m|
          @messages << m
        end
      end
    end
  end

  def stop
    @running = false
  end

  def next
    return @messages.shift
  end


  private
  def fetch_with_backoff
    parms = {}
    parms[:autoAck] = 'yes' unless @receive_options[:auto_ack].nil? || !@receive_options[:auto_ack]
    parms[:block] = 'yes' unless @receive_options[:block].nil? || !@receive_options[:block]
    parms[:count] = @receive_options[:count] unless @receive_options[:count].nil?
    parms[:timeout] = @receive_options[:timeout] unless @receive_options[:timeout].nil?
    parms[:redeliveryTimeout] = @receive_options[:redelivery_timeout] unless @receive_options[:redelivery_timeout].nil?

    while true
      begin
        result = Retry.do(10, 0.1) {
          response = HTTParty.get(
            "#{@url}/api/2/pipe/#{@pipe}",
              query: parms,
              headers: {
                "Content-Type": 'application/json',
                Authorization: "api #{@apikey}"
              }
          )
          js = response.parsed_response
          # puts js
          raise 'nothing to receive' if js['Events'].nil?
          return js['Events']
        }
      rescue => exception
        puts "could not fetch because #{exception}"
      end
    end
  end
end