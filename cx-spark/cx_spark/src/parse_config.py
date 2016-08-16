from collections import defaultdict
import ConfigParser

import os
import sys
def load_configuration():
	config_params = defaultdict(dict)
	cur_path = os.path.dirname(os.path.realpath(__file__))

	config = ConfigParser.RawConfigParser()
	settings_file = os.path.join(cur_path, 'conf/settings.cfg')
	config.read(settings_file)
	sections = config.sections()
	for section in sections:
		options = config.options(section)
		for option in options:
			val = config.get(section, option)
			if not val:
				print 'missing param %s %s '%(section, option)
				sys.exit('exitting now!')
			config_params[section][option] = config.get(section, option)
	return config_params
