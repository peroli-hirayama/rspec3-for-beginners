require 'time'
require 'models/active_record/campaign'
require 'models/mongoid/record_log'
require 'commands/calc_report/log'

module Commands
  module CalcReport
    module Logics
      module LogLineProcessor
        class Base
          def process(buf)
            logger = Log::logger
            sym = self.symbol

            # 開始時刻を記録
            start_at = DateTime.now

            # 最後に記録された時刻を取得
            if ENV["MERYAD_LAST_RECORD_SUP"]
        #      last_record = Time.parse('2015-03-12T00:00:00')
              last_record_sup = Time.parse(ENV["MERYAD_LAST_RECORD_SUP"])
            else
              last_record = Models::Mongoid::RecordLog.where(symbol: sym).order_by(:record_sup.desc).first
              last_record_sup = last_record.nil? ? Time.at(0) : last_record.record_sup
            end
            logger.info 'calc_report records from: ' + last_record_sup.to_s

            campaign_by_id = {}

            criteria = self.criteria(last_record_sup)

            # 現在の秒.00より小さいレコードに限定（カブりを防ぐため）
            last_one = criteria.last
            return buf if last_one.nil?
            sup = Time.at(last_one.time.to_i) # :00
            logger.debug sprintf("sup: %p", sup)

            # supがlast_record_sup + 1secより前なら置き換え
            # （いつまでも計上されないこと防止＋二重起動無し＆1秒以上の間隔開けて起動されること前提）
            if sup.to_time.to_i < last_record_sup.to_time.to_i + 1
              sup = Time.at(last_record_sup.to_time.to_i + 1)
            end

            cnt = 0
            criteria.lt(:time => sup).each { |r|
              # ここでデータ作る
              logger.debug sprintf("[%d]: %p", cnt, r)

              ca = nil
              if campaign_by_id.key?(r.CampaignID)
                ca = campaign_by_id[r.CampaignID]
              else
                Models::ActiveRecord::Campaign.with_readonly do
                  ca = Models::ActiveRecord::Campaign.find_by_id(r.CampaignID)
                end
                campaign_by_id[r.CampaignID] = ca
              end
              next if ca.nil?
        #      logger.debug sprintf("campaign_id: %d => adv_id: %d", ca.id, ca.advertiser_id)
              cnt = cnt + 1
              counter_args = {
                date:          r.time,
                advertiser_id: ca.advertiser_id,
                campaign_id:   r.CampaignID,
                publisher_id:  r.PublisherID,
                unit_id:       r.UnitID
              }

              if /BannerAd/ =~ r.AdType
                counter_args[:ad_id] = r.AdData["ID"]
                counter_args[:creative_id] = r.AdData["CreativeID"]
              elsif /ThirdAd/ =~ r.AdType
                counter_args[:ad_id] = r.AdData["ID"]
                counter_args[:creative_id] = r.AdData["CreativeID"]
              end

              buf.accumulate(sym, counter_args)
            }

            buf.record_log(sym,
              started_at:  start_at,
              record_from: last_record_sup,
              record_sup:  sup,
              recorded_at: DateTime.now
            )

            return buf
          end
        end
      end
    end
  end
end
