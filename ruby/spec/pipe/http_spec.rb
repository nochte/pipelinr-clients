require_relative '../../pipe/http'

describe HTTPPipe do
  before(:each) do
    @pipe = HTTPPipe.new(apikey: ENV['PIPELINR_API_KEY'], pipe: 'testpipe-ruby')
  end

  describe '.send' do
    it 'should blow up without payload' do
      expect { @pipe.send route: ['some']}.to raise_error
    end
    it 'should blow up without route' do
      expect { @pipe.send payload: '{"foo":"bar"}'}.to raise_error
    end

    it 'should return a 20x with payload and route' do  
      expect(@pipe.send(payload: '{"foo":"bar"}', route: ['testpipe-ruby', 'some', 'route', 'goes']).length).to_not equal 0
    end
  end

  # describe '.fetch_with_backoff' do
  #   it 'should return someting' do
  #     puts @pipe.fetch_with_backoff  
  #   end
    
  # end
end


# require_relative './pipe/http'
# threads = []
# pipe = HTTPPipe.new(apikey: ENV['PIPELINR_API_KEY'], pipe: 'testpipe-ruby')
# (1..50).each do |x|
#   threads << Thread.new do
#     (1..10000).each do |y|
#       Retry.do(100, 0.1) {
#         pipe.send(payload: '{"ndx":"'+ y.to_s + '","thread":"' + x.to_s + '"}', route: ['testpipe-ruby', 'some', 'route', 'goes'])
#       }
#     end
#   end
# end

# threads.each{|t| t.join}


# # ---------

require_relative './pipe/http'
threads = []
(1..50).each do
  threads << Thread.new do
    pipe = HTTPPipe.new(apikey: ENV['PIPELINR_API_KEY'], pipe: 'testpipe-ruby')
    pipe.receive_options = {auto_ack: false, count: 50}
    pipe.start
    while msg = pipe.next do
      Retry.do(100, 0.1) {
        puts 'completing ' + msg['id']['value']
        pipe.complete(msg['id']['value'])
      }
    end    
  end
end

# threads.each{|t| t.join}
