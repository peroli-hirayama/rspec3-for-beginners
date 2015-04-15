require 'commands/calc_report/logics/log_line_processor/base'
require 'models/mongoid/click_log_line'

module Commands
  module CalcReport
    module Logics
      module LogLineProcessor
        class Click < Base
          def symbol
            return :click
          end

          def criteria(start_time)
            return Models::Mongoid::ClickLogLine.gte(:time => start_time).order_by(:time.asc)
          end
        end
      end
    end
  end
end
