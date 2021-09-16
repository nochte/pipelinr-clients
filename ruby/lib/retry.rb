class Retry
  def self.do(count, backoffSeconds)
    laster = nil
    (1...count).each do |x|
      begin
        res = yield
        return res
      rescue => e
        laster = e
        puts laster
        sleep backoffSeconds * x
      end
    end
  
    raise laster
  end
end
