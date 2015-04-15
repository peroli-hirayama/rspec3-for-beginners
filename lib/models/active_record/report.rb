require 'active_record'
require 'switch_point'

module Models
  module ActiveRecord
    class Report < ::ActiveRecord::Base
      use_switch_point :ad
    end
  end
end
