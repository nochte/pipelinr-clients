require_relative '../../lib/retry'

describe Retry do
  describe '#do' do
    it 'should blow up if something always fails' do
      expect {
        Retry.do(10, 0.1) do
          raise 'eep!'
        end
      }.to raise_error
    end
    it 'should not blow up if something always succeeds' do
      expect(Retry.do(10, 0.1){
        5
      }).to be_equal(5)
    end
    it 'should fail sometimes, but eventually succeed' do
      ndx = 1
      expect(Retry.do(10, 0.1){
        ndx+=1
        raise 'nope' if ndx < 3
        ndx
      }).to be_equal(3)
    end
  end
end
