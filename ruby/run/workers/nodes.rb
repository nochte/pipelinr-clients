require_relative '../../lib/retry'
require_relative '../../worker/worker'

whichnodes = [    "node1",
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
  "node12",
]

whichnodes = ENV['NODES'].split(',') if ENV['NODES']

def launch_node(nodename) 
  Thread.new do
    worker = NewHTTPWorker(apikey: ENV['PIPELINR_API_KEY'], url: ENV['PIPELINR_URL'], step: nodename)
    worker.receive_options = {redelivery_timeout: 60*10}

    i = 0
    worker.on_message{|message|
      puts "(#{nodename}) handling #{message['id']['value']}"
      Retry.do(10, 0.250) {
        worker.pipe.log(message['id']['value'], 1, 'decoration added')
      }
      Retry.do(10, 0.250) {
        worker.pipe.decorate(message['id']['value'], [{Key: nodename, Value: '"f"'}])
      }
      i += 1
    }

    worker.run()
  end
end

whichnodes.map{|n| launch_node(n)}.map(&:join)