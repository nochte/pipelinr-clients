require_relative '../../lib/retry'
require_relative '../../worker/worker'

numthreads = 1
numthreads = ENV['THREADS'].to_i unless ENV['THREADS'].nil?

threads = []
numthreads.times do 
  worker = NewHTTPWorker(apikey: ENV['PIPELINR_API_KEY'], url: ENV['PIPELINR_URL'], step: 'route')
  worker.receive_options = {redelivery_timeout: 60*10}

  worker.on_message{|message|
    newroute = [
      "node1",
      "node2",
      "node3",
      "node4",
      "node5",
      "node6",
      "node7",
      "node8",
      "node9",
      "node10",
      "node11",
      "node12"
    ].shuffle()[0..(rand*12).floor]
    newroute << 'graph'
    Retry.do(10, 0.25) {
      puts "adding step to #{message['id']['value']}"
      worker.pipe.add_steps(message['id']['value'], newroute)
    }
  }
  threads << Thread.new do 
    worker.run
  end
end

threads.each{|t| t.join}