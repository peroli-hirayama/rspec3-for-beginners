require 'commands/calc_report/log'

module Commands
  module CalcReport
    module Logics
      class CountBuffer
        def initialize
          @counter = {}
          @logs = {}
        end
      
        def accumulate(sym, args)
          logger = Log::logger
          logger.debug sprintf('# accumulate(%s): %p', sym, args)
      #    date = args[:date].to_time.to_date
          date = args[:date].to_date
          logger.debug sprintf('[MARK] UnitID=%d, %p -> %s', args[:unit_id], args[:date], date)
          @counter[date] ||= {}
          @counter[date][args[:advertiser_id]] ||= {}
          @counter[date][args[:advertiser_id]][args[:campaign_id]] ||= {}
          @counter[date][args[:advertiser_id]][args[:campaign_id]][args[:creative_id]] ||= {}
          @counter[date][args[:advertiser_id]][args[:campaign_id]][args[:creative_id]][args[:ad_id]] ||= {}
          @counter[date][args[:advertiser_id]][args[:campaign_id]][args[:creative_id]][args[:ad_id]][args[:publisher_id]] ||= {}
          @counter[date][args[:advertiser_id]][args[:campaign_id]][args[:creative_id]][args[:ad_id]][args[:publisher_id]][args[:unit_id]] ||= {}
          @counter[date][args[:advertiser_id]][args[:campaign_id]][args[:creative_id]][args[:ad_id]][args[:publisher_id]][args[:unit_id]][sym] ||= 0
          @counter[date][args[:advertiser_id]][args[:campaign_id]][args[:creative_id]][args[:ad_id]][args[:publisher_id]][args[:unit_id]][sym] += 1
        end
      
        def each
          @counter.each do |date, date_v|
            date_v.each do |adv_id, adv_v|
              adv_v.each do |ca_id, ca_v|
                ca_v.each do |cr_id, cr_v|
                  cr_v.each do |ad_id, ad_v|
                    ad_v.each do |pub_id, pub_v|
                      pub_v.each do |unit_id, unit_v|
                        counts = {
                          date:             date,
                          advertiser_id:    adv_id,
                          campaign_id:      ca_id,
                          creative_id:      cr_id,
                          ad_id:            ad_id,
                          publisher_id:     pub_id,
                          unit_id:          unit_id,
                          impression_count: unit_v[:deliver] || 0,
                          click_count:      unit_v[:click] || 0,
                        }
                        yield counts
                      end
                    end
                  end
                end
              end
            end
          end
        end
      
        def record_log(sym, args)
          @logs[sym] = args
        end
      
        attr_reader :logs
      end
    end
  end
end
