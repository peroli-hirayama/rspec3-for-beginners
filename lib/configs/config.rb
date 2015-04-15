require 'yaml'

module Configs
  module Config
    EXEC_ENV = ( ENV["MERYAD_EXEC_ENV"] ) == 'production' ? :production : :development
    BATCH_PATH = ENV["MERYAD_BATCH_PATH"] || File.join(ENV["HOME"], 'mery_ad_batch')

    def self.prepare
      return if defined? @@config

      @@config = {
        database:    YAML.load_file(File.join(BATCH_PATH, 'config/database.yml')),
        mongoid_yml: File.join(BATCH_PATH, 'config/mongoid.yml')
      }
    end

    def self.get(s)
      return @@config[s]
    end
  end
end
