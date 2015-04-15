require 'configs/type'
require 'models/active_record/campaign'
require 'models/mongoid/record_log'

require 'commands/calc_report/logics/count_buffer'
require 'commands/calc_report/logics/log_line_processor/deliver'
require 'commands/calc_report/logics/log_line_processor/click'

module Commands
  module CalcReport
    class Command
      def self.run(argv)
        new(argv).execute
      end

      def initialize(argv)
        @argv = argv
      end

      def execute
        # ここにコード本体が入る
        logger = Log::logger

        logger.info 'process started.'

        logger.debug 'EXEC_ENV: ' + Configs::Config::EXEC_ENV.to_s
        logger.debug 'BATCH_PATH: ' + Configs::Config::BATCH_PATH.to_s

        count_buffer = Logics::CountBuffer.new
        logger.info 'process deliver logs...'
        Logics::LogLineProcessor::Deliver.new.process(count_buffer)
        logger.info 'process click logs...'
        Logics::LogLineProcessor::Click.new.process(count_buffer)

        count_buffer.each do |counter|
          # calc spend
          campaign = nil
          Models::ActiveRecord::Campaign.with_readonly do
            campaign = Models::ActiveRecord::Campaign.find_by_id(counter[:campaign_id])
          end
          next if campaign.nil?

          spend = case campaign.bid_charge_type
                    when Configs::Type::BidChargeType::CPC
                      counter[:click_count] * campaign.bid_price
                    when Configs::Type::BidChargeType::CPM
                      counter[:impression_count] * campaign.bid_price / 1000
                    else
                      0
                    end

          # insert as mysql records
          Models::ActiveRecord::Report.with_writable do
            Models::ActiveRecord::Report.create!(
              date:             counter[:date],
              advertiser_id:    counter[:advertiser_id],
              campaign_id:      counter[:campaign_id],
              creative_id:      counter[:creative_id],
              ad_id:            counter[:ad_id],
              publisher_id:     counter[:publisher_id],
              unit_id:          counter[:unit_id],
              impression_count: counter[:impression_count],
              click_count:      counter[:click_count],
              spend:            spend,
            )
          end
          logger.info sprintf('insert record to reports: counter=%p, spend=%d', counter, spend)
        end

        count_buffer.logs.each do |sym, log|
          Models::Mongoid::RecordLog.create!(
              started_at:  log[:started_at],
              symbol:      sym,
              record_from: log[:record_from],
              record_sup:  log[:record_sup],
              recorded_at: log[:recorded_at]
          )
        end

        logger.info 'process is completed.'
      end
    end
  end
end
