# -*- encoding: utf-8 -*-
 
Gem::Specification.new do |s|
  s.name = %q{collectd}
  s.version = "0.0.5"
 
  s.authors = ["Stephan Maka"]
  s.date = %q{2009-05-01}
  s.email = %q{stephan@spaceboyz.net}
  s.files = %w(
lib/collectd/interface.rb
lib/collectd/proc_stats.rb
lib/collectd/pkt.rb
lib/collectd/server.rb
lib/collectd/em_server.rb
lib/collectd.rb
)
  s.has_rdoc = false # Insufficient
  s.homepage = %q{http://github.com/astro/ruby-collectd}
  s.require_paths = ["lib"]
  s.summary = %q{Send collectd statistics from Ruby}
end
