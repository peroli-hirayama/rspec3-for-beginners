require 'time'
require 'mongoid'

module Models
  module Mongoid
    class RecordLog
        include ::Mongoid::Document
        field :started_at,   :type => DateTime
        field :written_data, :type => Hash
        field :symbol,       :type => String
        field :record_from,  :type => DateTime
        field :record_sup,   :type => DateTime
        field :recorded_at,  :type => DateTime
        store_in collection: "record_logs"
    end
  end
end
