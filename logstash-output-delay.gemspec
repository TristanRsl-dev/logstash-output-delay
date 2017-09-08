Gem::Specification.new do |s|
  s.name          = 'logstash-output-delay'
  s.version       = '0.1.0'
  s.licenses      = ['Apache-2.0']
  s.summary       = 'Apply a delay on each event and send them either in Elasticsearch, either on stdout.'
  s.description   = 'For each event passing through the plugin, a delay is apply on them. Once this delay is over, the event is send to Elasticsearch or to stdout using the plugin logstash-output-elasticsearch and logstash-output-stdout.'
  s.homepage      = 'https://github.com/TristanRsl-dev/logstash-output-delay'
  s.authors       = ['Tristan Roussel']
  s.email         = 'tristan.roussel@epita.fr'
  s.require_paths = ['lib']

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", "~> 2.0"
  s.add_runtime_dependency "logstash-codec-plain"
  s.add_runtime_dependency "logstash-output-stdout"
  s.add_runtime_dependency "logstash-output-elasticsearch"
  s.add_development_dependency "logstash-devutils"
end
