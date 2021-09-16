require_relative '../../pipe/http'

numpipes = 500
count = 0

unless ENV['COUNT'].nil?
  count = ENV['COUNT'].to_i
end

persec = 1
unless ENV['PER_SECOND'].nil?
  persec = ENV['PER_SECOND'].to_f
end

puts "#{persec}: #{count}"
mux = Mutex.new
buffer = Queue.new
donebuffer = Queue.new
threads = []

# threads << Thread.new do
#   st = Time.now
#   count = 0
#   while true
#     if donebuffer.length == 0
#       sleep(1)
#       next
#     end
#     donebuffer.deq
#     count += 1
#     if count % 100 == 0
#       elapsed = Time.now - st
#       puts "(#{count}) #{count.to_f / elapsed} per sec"
#     end
#   end
# end

numpipes.times do 
  threads << Thread.new do
    pipe = HTTPPipe.new(apikey: ENV['PIPELINR_API_KEY'], pipe: 'testpipe-ruby')
    while true do
      donebuffer << pipe.send(buffer.deq)
    end
  end
end




timestep = 1.0/persec.to_f
i = 0
while (count == 0 || i < count) do
  i+=1
  buffer << ({
    route: ['route'],
    payload: %Q{{"index":#{i}}}
  })
  sleep timestep
end

threads.each{|t| t.join}