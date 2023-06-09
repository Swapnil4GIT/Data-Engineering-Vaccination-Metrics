#----------------------------------------------------------------------------------------------------------#
# This python program is to create the default configuration file at the beginning of every job run.       #
# If you need to make any configuration changes related to constant variables, please make it here.        #
#----------------------------------------------------------------------------------------------------------#

import configparser

class generate_config:

    def __init__(self):
        config = configparser.ConfigParser()

        config.add_section('parameters')
        config.set('parameters', 
                'filepath', 
                '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Data/')
        config.set('parameters',
                'host',
                'localhost')
        config.set('parameters',
                'hostuser',
                'hostpassword')
        config.set('parameters',
                'logpath',
                '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Logs/')
        config.set('parameters',
                    'configfilepath',
                    '/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Config/configfile.ini')

        with open(r'/Volumes/E-Study/Github/Data-Engineering-Vaccination-Metrics/Config/configfile.ini',
                'w') as configfile:
            config.write(configfile)

