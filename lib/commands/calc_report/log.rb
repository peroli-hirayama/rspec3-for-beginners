require 'logger'
require 'configs/config'

module Commands
  module CalcReport
    class Log
      @@logger = nil
      def self.logger
        if @@logger.nil?
          @@logger = Logger.new(File.join(Configs::Config::BATCH_PATH, 'log/calc_report.log'))
          if ( Configs::Config::EXEC_ENV == :production )
            @@logger.level = Logger::INFO
          end
          @@logger.progname = 'calc_report.rb'
        end
        return @@logger
      end
    end
  end
end
