require 'mongoid'
require 'time'
require 'active_record'
require 'mysql2'
require 'yaml'
require 'switch_point'
require 'logger'

# Type

module BidChargeType
  CPC = 1
  CPM = 2
end

class Log
  @@logger = nil
  def self.logger
    if @@logger.nil? then
      @@logger = Logger.new(File.join(ENV["MERYAD_BATCH_PATH"], 'log/calc_report.log'))
      if ( ENV["MERYAD_EXEC_ENV"] == "production" ) then
        @@logger.level = Logger::INFO
        @@logger.progname = 'calc_report.rb'
      end
    end
    return @@logger
  end
end

# Environments

if ( ENV["MERYAD_EXEC_ENV"] == "production" ) then
  is_production = true
else
  ENV["MERYAD_EXEC_ENV"] = "development"
end

ENV["MERYAD_BATCH_PATH"] ||= File.join(ENV["HOME"], 'mery_ad_batch')

# ActiveRecord

ActiveRecord::Base.configurations = YAML.load_file(File.join(ENV["MERYAD_BATCH_PATH"], 'config/database.yml'))
ActiveRecord::Base.establish_connection(:"#{ENV["MERYAD_EXEC_ENV"]}_ad_master")
ActiveRecord::Base.establish_connection(:"#{ENV["MERYAD_EXEC_ENV"]}_ad_slave")

logger = Log::logger
logger.info 'info level log..'

SwitchPoint.configure do |config|
  config.define_switch_point :ad,
    readonly: :"#{ENV["MERYAD_EXEC_ENV"]}_ad_master",
    writable: :"#{ENV["MERYAD_EXEC_ENV"]}_ad_slave"
end

class Campaign < ActiveRecord::Base
  use_switch_point :ad
end

class Report < ActiveRecord::Base
  use_switch_point :ad
end

# Mongoid

mongoid_yml = File.join(ENV["MERYAD_BATCH_PATH"], 'config/mongoid.yml')
if is_production then
  Mongoid.load!(mongoid_yml, :production)
else
  Mongoid.load!(mongoid_yml, :development)
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

class RecordLog
    include Mongoid::Document
    field :started_at, :type => DateTime
    field :written_data, :type => Hash
    field :symbol, :type => String
    field :recorded_at, :type => DateTime
#    store_in collection: "record_log"
end

# Other classes

class LogLineProcessor
  def process(buf)
    sym = self.symbol

    # 開始時刻を記録
    start_at = DateTime.now

    # 最後に記録された時刻を取得
    if ENV["MERYAD_LAST_RECORDED_AT"] then
#      last_record = Time.parse('2015-03-12T00:00:00')
      last_record = Time.parse(ENV["MERYAD_LAST_RECORDED_AT"])
    else
      last_record = RecordLog.where(symbol: sym).order_by(:recorded_at.desc).first.recorded_at
    end
#    p last_record

    campaign_by_id = {}

    # 現在の秒.00より小さいレコードに限定
    crit = self.criteria(last_record)

    last_one = crit.last
    return buf if last_one.nil?
    sup = Time.at(last_one.time.to_i) # :00

    cnt = 0
    crit.lt(:time => sup).each { |r|
      # ここでデータ作る
#      printf("[%d]: %p\n", cnt, r)

      ca = nil
      if campaign_by_id.key?(r.CampaignID) then
        ca = campaign_by_id[r.CampaignID]
      else
        Campaign.with_readonly do
          ca = Campaign.find_by_id(r.CampaignID)
        end
        campaign_by_id[r.CampaignID] = ca
      end
      next if ca.nil?
#      printf("campaign_id: %d => adv_id: %d\n", ca.id, ca.advertiser_id)
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

class ClickLogLineProcessor < LogLineProcessor
  def symbol
    return "click"
  end

  def criteria(last_time)
    return ClickLogLine.gte(:time => last_time).order_by(:time.asc)
  end
end

class CountBuffer
  def initialize
    @counter = {}
  end

  def accumulate(sym, args)
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

# Main processes

count_buffer = CountBuffer.new
DeliverLogLineProcessor.new.process(count_buffer)
ClickLogLineProcessor.new.process(count_buffer)

count_buffer.each do |c|
  # calc spend
  spend = 0
  ca = nil
  Campaign.with_readonly do
    ca = Campaign.find_by_id(c[:campaign_id])
  end
  next if ca.nil?

  case ca.bid_charge_type
  when BidChargeType::CPC
    spend = c[:click_count] * ca.bid_price
  when BidChargeType::CPM
    spend = c[:impression_count] * ca.bid_price / 1000
  else
  end

  # insert as mysql records
  Report.with_writable do
    Report.create!(
      date: c[:date],
      advertiser_id: c[:advertiser_id],
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
end
