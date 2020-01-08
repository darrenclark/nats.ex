defmodule Gnat.Streaming.Subscription do

  defstruct [
    :inbox,
    :ack_inbox,
    :subject,
    :durable_name,
    :gnat_sub,
    :pid
  ]

  alias __MODULE__
  alias Gnat.Streaming.Pb.SubscriptionRequest, as: SubReq
  alias Gnat.Streaming.Pb.SubscriptionResponse, as: SubResp

  def new(%SubReq{} = sub_req, %SubResp{error: ""} = sub_resp, gnat_sub, pid) do
    %Subscription{
      inbox: sub_req.inbox,
      ack_inbox: sub_resp.ackInbox,
      subject: sub_req.subject,
      durable_name: sub_req.durableName,
      gnat_sub: gnat_sub,
      pid: pid
    }
  end
end
