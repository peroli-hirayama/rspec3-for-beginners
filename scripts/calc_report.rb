require 'mongoid'
require 'time'
require 'active_record'
require 'mysql2'

Mongoid.load!('config/mongoid.yml', :development)

module BidChargeType
  CPC = 1
  CPM = 2
end

class DeliverLogLine
    include Mongoid::Document
    field :AudienceID, :type => String
    field :UserAgent, :type => String
    field :RemoteAddr, :type => String
    field :SessionID, :type => String
#    field :Timestamp, :type => DateTime
    field :MediaURL, :type => String
    field :ImpressionID, :type => String
    field :AdType, :type => String
    field :CampaignID, :type => Integer
    field :PublisherID, :type => Integer
    field :UnitID, :type => Integer
    field :DeliverLogicID, :type => Integer
    field :NativePublisherCreativeID, :type => Integer
    field :AdData, :type => Hash # 複雑化するならクラス化する
    field :time, :type => DateTime
    store_in collection: "deliver_logs"
end

class ClickLogLine
    include Mongoid::Document
    field :AudienceID, :type => String
    field :UserAgent, :type => String
    field :RemoteAddr, :type => String
    field :SessionID, :type => String
#    field :Timestamp, :type => DateTime
    field :MediaURL, :type => String
    field :ImpressionID, :type => String
    field :AdType, :type => String
    field :CampaignID, :type => Integer
    field :PublisherID, :type => Integer
    field :UnitID, :type => Integer
    field :DeliverLogicID, :type => Integer
    field :NativePublisherCreativeID, :type => Integer
    field :AdData, :type => Hash # 複雑化するならクラス化する
    field :time, :type => DateTime
    store_in collection: "click_logs"
end

class LogLineProcessor
  def process(buf)
    sym = self.symbol

    # 開始時刻を記録
    start_at = DateTime.now

    # 最後に記録された時刻を取得
#    last_record = RecordLog.where(symbol: sym).order_by(:recorded_at.desc).first.recorded_at
    last_record = Time.parse('2015-03-12T00:00:00')

    campaign_by_id = {}

    cnt = 0
    self.criteria(last_record).each { |r|
      # ここでデータ作る
      printf("[%d]: %p\n", cnt, r)
    
      if campaign_by_id.key?(r.CampaignID) then
        ca = campaign_by_id[r.CampaignID]
    #    p '# cache hit'
      else
        ca = Campaign.find_by_id(r.CampaignID)
        campaign_by_id[r.CampaignID] = ca
      end
      next if ca.nil?
      printf("campaign_id: %d => adv_id: %d\n", ca.id, ca.advertiser_id)
      cnt = cnt + 1
    
      counter_args = {
        :date => r.time,
        :advertiser_id => ca.advertiser_id,
        :campaign_id => r.CampaignID,
        :publisher_id => r.PublisherID,
        :unit_id => r.UnitID
      }
    
      if /BannerAd/ =~ r.AdType then
        counter_args[:ad_id] = r.AdData["ID"]
        counter_args[:creative_id] = r.AdData["CreativeID"]
      elsif /ThirdAd/ =~ r.AdType then
        counter_args[:ad_id] = r.AdData["ID"]
        counter_args[:creative_id] = r.AdData["CreativeID"]
      end
    
      buf.accumulate(sym, counter_args)
    }

    RecordLog.create!(
      started_at: start_at,
      written_data: {:count => 100},
      symbol: sym,
      recorded_at: DateTime.now
    )
    return buf
  end
end

class DeliverLogLineProcessor < LogLineProcessor
  def symbol
    return "deliver"
  end

  def criteria(last_time)
    return DeliverLogLine.gte(:time => last_time).order_by(:time.asc)
  end
end

class DeliverLogLineProcessor < LogLineProcessor
  def symbol
    return "click"
  end

  def criteria(last_time)
    return ClickLogLine.gte(:time => last_time).order_by(:time.asc)
  end
end

class RecordLog
    include Mongoid::Document
    field :started_at, :type => DateTime
    field :written_data, :type => Hash
    field :symbol, :type => String
    field :recorded_at, :type => DateTime
#    store_in collection: "record_log"
end

ActiveRecord::Base.establish_connection(
  :adapter => 'mysql2',
  :host => 'localhost',
  :username => 'adpf_user',
  :password => 'sd98udha73a2vm',
  :database => 'mery_adpf',
)

class Campaign < ActiveRecord::Base
end

class Report < ActiveRecord::Base
end

class CountBuffer
  def initialize
    @counter = {}
  end

  def accumulate(sym, args)
    p sym
    date = args[:date].to_date
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
                    :date => date,
                    :advertiser_id => adv_id,
                    :campaign_id => ca_id,
                    :creative_id => cr_id,
                    :ad_id => ad_id,
                    :publisher_id => pub_id,
                    :unit_id => unit_id,
                    :impression_count => unit_v["deliver"],
                    :click_count => unit_v["click"] || 0,
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
end

count_buffer = CountBuffer.new
DeliverLogLineProcessor.new.process(count_buffer)
ClickLogLineProcessor.new.process(count_buffer)

count_buffer.each do |c|
  # spendを計算する
  spend = 0
  ca = Campaign.find_by_id(c[:campaign_id])
  next if ca.nil? 

  case ca.bid_charge_type
  when BidChargeType::CPC
    spend = c[:click_count] * ca.bid_price
  when BidChargeType::CPM
    spend = c[:impression_count] * ca.bid_price / 1000
  else
  end

  # ここでMySQLに加算
  Report.create!(
    date: c[:date],
    advertier_id: c[:advertiser_id],
    campaign_id: c[:campaign_id],
    creative_id: c[:creative_id],
    ad_id: c[:ad_id],
    publisher_id: c[:publisher_id],
    unit_id: c[:unit_id],
    impression_count: c[:impression_count],
    click_count: c[:click_count],
    spend: spend,
  )
end
