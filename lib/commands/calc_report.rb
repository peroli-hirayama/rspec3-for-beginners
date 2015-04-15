require 'configs/config'
require 'configs/db'
require 'configs/mongoid'

Configs::Config::prepare
Configs::DB.prepare

require 'models/active_record/campaign'
require 'models/active_record/report'

Configs::Mongoid.prepare
