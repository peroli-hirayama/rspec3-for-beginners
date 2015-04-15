require 'active_record'
require 'switch_point'

module Models
  module ActiveRecord
    class Campaign < ::ActiveRecord::Base
      use_switch_point :ad
    end
  end
end
