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
    if @@logger.nil?
      @@logger = Logger.new(File.join(ENV["MERYAD_BATCH_PATH"], 'log/calc_report.log'))
      if ( ENV["MERYAD_EXEC_ENV"] == "production" )
        @@logger.level = Logger::INFO
      end
      @@logger.progname = 'calc_report.rb'
    end
    return @@logger
  end
end

# Environments

if ( ENV["MERYAD_EXEC_ENV"] == "production" )
  is_production = true
else
  ENV["MERYAD_EXEC_ENV"] = "development"
end

ENV["MERYAD_BATCH_PATH"] ||= File.join(ENV["HOME"], 'mery_ad_batch')

logger = Log::logger

logger.info 'process started.'

logger.debug 'EXEC_ENV: ' + ENV["MERYAD_EXEC_ENV"]
logger.debug 'MERYAD_BATCH_PATH: ' + ENV["MERYAD_BATCH_PATH"]

# ActiveRecord

ActiveRecord::Base.configurations = YAML.load_file(File.join(ENV["MERYAD_BATCH_PATH"], 'config/database.yml'))
ActiveRecord::Base.establish_connection(:"#{ENV["MERYAD_EXEC_ENV"]}_ad_master")
ActiveRecord::Base.establish_connection(:"#{ENV["MERYAD_EXEC_ENV"]}_ad_slave")

logger.debug 'ActiveRecord configurations done.'

SwitchPoint.configure do |config|
  config.define_switch_point :ad,
    readonly: :"#{ENV["MERYAD_EXEC_ENV"]}_ad_slave",
    writable: :"#{ENV["MERYAD_EXEC_ENV"]}_ad_master"
end

class Campaign < ActiveRecord::Base
  use_switch_point :ad
end

class Report < ActiveRecord::Base
  use_switch_point :ad
end

# Mongoid

mongoid_yml = File.join(ENV["MERYAD_BATCH_PATH"], 'config/mongoid.yml')
if is_production
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
    field :LoggedAt, :type => DateTime
    field :DeliveredAt, :type => DateTime
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
    store_in collection: "deliver"
end

class ClickLogLine
    include Mongoid::Document
    field :AudienceID, :type => String
    field :UserAgent, :type => String
    field :RemoteAddr, :type => String
    field :SessionID, :type => String
    field :LoggedAt, :type => DateTime
    field :DeliveredAt, :type => DateTime
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
    store_in collection: "click"
end

class RecordLog
    include Mongoid::Document
    field :started_at, :type => DateTime
    field :written_data, :type => Hash
    field :symbol, :type => String
    field :record_from, :type => DateTime
    field :record_sup, :type => DateTime
    field :recorded_at, :type => DateTime
#    store_in collection: "record_log"
end

# Other classes

class LogLineProcessor
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
      last_record = RecordLog.where(symbol: sym).order_by(:record_sup.desc).first
      last_record_sup = last_record.nil? ? Time.at(0) : last_record.record_sup
    end
    logger.info 'calc_report records from: ' + last_record_sup.to_s

    campaign_by_id = {}

    crit = self.criteria(last_record_sup)

    # 現在の秒.00より小さいレコードに限定（カブりを防ぐため）
    last_one = crit.last
    return buf if last_one.nil?
    sup = Time.at(last_one.time.to_i) # :00
    logger.debug sprintf("sup: %p", sup)

    # supがlast_record_sup + 1secより前なら置き換え
    # （いつまでも計上されないこと防止＋二重起動無し＆1秒以上の間隔開けて起動されること前提）
    if sup.to_time.to_i < last_record_sup.to_time.to_i + 1
      sup = Time.at(last_record_sup.to_time.to_i + 1)
    end

    cnt = 0
    crit.lt(:time => sup).each { |r|
      # ここでデータ作る
      logger.debug sprintf("[%d]: %p", cnt, r)

      ca = nil
      if campaign_by_id.key?(r.CampaignID)
        ca = campaign_by_id[r.CampaignID]
      else
        Campaign.with_readonly do
          ca = Campaign.find_by_id(r.CampaignID)
        end
        campaign_by_id[r.CampaignID] = ca
      end
      next if ca.nil?
#      logger.debug sprintf("campaign_id: %d => adv_id: %d", ca.id, ca.advertiser_id)
      cnt = cnt + 1
      counter_args = {
        :date => r.time,
        :advertiser_id => ca.advertiser_id,
        :campaign_id => r.CampaignID,
        :publisher_id => r.PublisherID,
        :unit_id => r.UnitID
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
      started_at: start_at,
      record_from: last_record_sup,
      record_sup: sup,
      recorded_at: DateTime.now
    )

    return buf
  end
end

class DeliverLogLineProcessor < LogLineProcessor
  def symbol
    return "deliver"
  end

  def criteria(start_time)
    return DeliverLogLine.gte(:time => start_time).order_by(:time.asc)
  end
end

class ClickLogLineProcessor < LogLineProcessor
  def symbol
    return "click"
  end

  def criteria(start_time)
    return ClickLogLine.gte(:time => start_time).order_by(:time.asc)
  end
end

class CountBuffer
  def initialize
    @counter = {}
    @logs = {}
  end

  def accumulate(sym, args)
    logger = Log::logger
    logger.debug sprintf('# accumulate(%s): %p', sym, args)
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
                    :impression_count => unit_v["deliver"] || 0,
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

  def record_log(sym, args)
    @logs[sym] = args
  end

  attr_reader :logs
end

# Main processes

count_buffer = CountBuffer.new
logger.info 'process deliver logs...'
DeliverLogLineProcessor.new.process(count_buffer)
logger.info 'process click logs...'
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

  logger.info sprintf('insert record to reports: c=%p, spend=%d', c, spend)
end

count_buffer.logs.each{ |sym, log|
  RecordLog.create!(
      started_at: log[:started_at],
      symbol: sym,
      record_from: log[:record_from],
      record_sup: log[:record_sup],
      recorded_at: log[:recorded_at]
  )
}

logger.info 'process is completed.'
