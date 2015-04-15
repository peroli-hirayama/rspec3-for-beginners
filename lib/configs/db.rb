require 'active_record'
require 'switch_point'
require 'configs/config'

module Configs
  module DB
    def self.prepare
      env = Config::EXEC_ENV.to_s
      connect_database env
      config_switchpoint env
    end

    def self.connect_database(env = Config::EXEC_ENV.to_s)
      ActiveRecord::Base.configurations = Config.get(:database)
#      p ActiveRecord::Base.configurations
      ActiveRecord::Base.establish_connection(:"#{env}_ad_master")
      ActiveRecord::Base.establish_connection(:"#{env}_ad_slave")
    end

    def self.config_switchpoint(env = Config::EXEC_ENV.to_s)
      SwitchPoint.configure do |config|
        config.define_switch_point :ad,
          readonly: :"#{env}_ad_slave",
          writable: :"#{env}_ad_master"
      end
    end
  end
end
