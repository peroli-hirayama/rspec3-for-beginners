require 'time'
require 'mongoid'

module Models
  module Mongoid
    class ClickLogLine
        include ::Mongoid::Document
        field :AudienceID,                :type => String
        field :UserAgent,                 :type => String
        field :RemoteAddr,                :type => String
        field :SessionID,                 :type => String
        field :LoggedAt,                  :type => DateTime
        field :DeliveredAt,               :type => DateTime
        field :MediaURL,                  :type => String
        field :ImpressionID,              :type => String
        field :AdType,                    :type => String
        field :CampaignID,                :type => Integer
        field :PublisherID,               :type => Integer
        field :UnitID,                    :type => Integer
        field :DeliverLogicID,            :type => Integer
        field :NativePublisherCreativeID, :type => Integer
        field :AdData,                    :type => Hash      # 複雑化するならクラス化する
        field :time,                      :type => DateTime
        store_in collection: "click"
    end
  end
end
