lib = File.expand_path('lib', __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'postgres_to_redshift/version'

Gem::Specification.new do |spec|
  spec.name          = 'postgres_to_redshift'
  spec.version       = PostgresToRedshift::VERSION
  spec.authors       = ['Alex Rakoczy']
  spec.email         = ['arakoczy@gmail.com']
  spec.summary       = 'Load postgres databases into Amazon Redshift'
  spec.description   = "Load postgres databases into Amazon Redshift. It's designed to work on Heroku Scheduler, or other *nix/BSD hosts."
  spec.homepage      = 'https://github.com/toothrot/postgres_to_redshift'
  spec.license       = 'MIT'

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_development_dependency 'bundler', '~> 1.6'
  spec.add_development_dependency 'pry', '~> 0.12.2'
  spec.add_development_dependency 'rake', '~> 10.0'
  spec.add_development_dependency 'rubocop', '~> 0.66'
  spec.add_dependency 'aws-sdk-v1', '~> 1.54'
  spec.add_dependency 'pg', '>= 0.18.1'
  spec.add_dependency 'pidfile'
end
