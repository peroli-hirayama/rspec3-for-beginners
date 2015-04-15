require 'mongoid'
require 'configs/config'

module Configs
  module Mongoid
    def self.prepare
      ::Mongoid.load!(Config.get(:mongoid_yml), Config::EXEC_ENV)
    end
  end
end
