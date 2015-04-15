require 'commands/calc_report/logics/log_line_processor/base'
require 'models/mongoid/deliver_log_line'

module Commands
  module CalcReport
    module Logics
      module LogLineProcessor
        class Deliver < Base
          def symbol
            return :deliver
          end

          def criteria(start_time)
            return Models::Mongoid::DeliverLogLine.gte(:time => start_time).order_by(:time.asc)
          end
        end
      end
    end
  end
end
