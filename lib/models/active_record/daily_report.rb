require 'active_record'
require 'switch_point'

module Models
  module ActiveRecord
    class DailyReport < ::ActiveRecord::Base
      use_switch_point :ad
    end
  end
end
