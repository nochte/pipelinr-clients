require_relative '../pipe/http'
require_relative '../lib/retry'

class Worker
  attr_reader :pipe
  def initialize(pipe:, step:) 
    @pipe = pipe
    @step = step
    @running = false

    @on_message_handlers = []
    @on_error_handlers = []
    
    @pipe.receive_options = {
      auto_ack: false,
      block: false,
      count: 10,
      timeout: 60,
      redelivery_timeout: 120
    }
  end

  def receive_options=(auto_ack: nil, block: nil, count: nil, timeout: nil, redelivery_timeout: nil)
    @pipe.receive_options={auto_ack: auto_ack, block: block, count: count, timeout: timeout, redelivery_timeout: redelivery_timeout}
  end

  def on_message(&block)
    @on_message_handlers << block if block
  end

  def on_error(&block)
    @on_error_handlers << block if block
  end

  def stop
    @running = false
  end

  def run
    raise 'already running' if @running
    @running = true
    @pipe.start
    while true
      while pipe.length == 0
        sleep 0.1
      end
      msg = pipe.next
      shouldcomplete = true
      keepon = true
      @on_message_handlers.each do |block|
        break unless keepon
        begin
          block.call(msg)
        rescue => exception
          puts "error on mesage: #{exception.to_s}"
          keepon = false

          Retry.do(40, 0.25) {
            @pipe.log(msg['id']['value'], -1, "failed to complete handler with error #{exception.to_s}")
          }
          if @on_error_handlers.length > 0 
            shouldcomplete = false
          end
          @on_error_handlers.each do |erblock|
            erblock.call(msg, exception)
          end
        end
      end

      if shouldcomplete
        Retry.do(40, 0.250) {
          @pipe.log(msg['id']['value'], 0, "completed step #{@step}")
        }
        Retry.do(40, 0.250) {
          @pipe.complete(msg['id']['value'])
        }
      end
    end
  end
end

def NewHTTPWorker(url:,apikey:,step:)
  pipe = HTTPPipe.new(apikey: apikey, pipe: step, url: url)
  Worker.new(pipe: pipe, step: step)
end
