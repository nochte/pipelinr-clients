# TODO: protobufisize this

# string Payload = 1;
# string MessageType = 2;
# repeated string Route = 3;
# repeated string CompletedSteps = 4;
# repeated RouteLog RouteLog = 5;
# string DecoratedPayload = 6 [(gogoproto.moretags) = "json:\"_,omitempty\" bson:\"_,omitempty\"" ];
# repeated Decoration Decorations = 7;

class Message
  attr_reader :payload, :route, :completed_steps, :route_log, 
  def initialize(messagejs, )
end