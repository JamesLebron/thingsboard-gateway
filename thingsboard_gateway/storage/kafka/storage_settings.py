# ------------------------------------------------------------------------------
#      Copyright 2025. ThingsBoard
#  #
#      Licensed under the Apache License, Version 2.0 (the "License");
#      you may not use this file except in compliance with the License.
#      You may obtain a copy of the License at
#  #
#          http://www.apache.org/licenses/LICENSE-2.0
#  #
#      Unless required by applicable law or agreed to in writing, software
#      distributed under the License is distributed on an "AS IS" BASIS,
#      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#      See the License for the specific language governing permissions and
#      limitations under the License.
#
# ------------------------------------------------------------------------------

class StorageSettings:
    def __init__(self, config):
        if isinstance(config.get('bootstrap_servers'), dict):
            self.__bootstrap_servers = config.get('bootstrap_servers').values()
        else:
            self.__bootstrap_servers = config.get('bootstrap_servers', ["localhost:9092"])
        self.__topic = config.get('topic', 'gateway.uplink')
        self.__partitions = config.get('partitions', 3)
        self.__replication_factor = config.get('replication_factor', 1)
        self.__replica_assignments = config.get('replica_assignments', None)
        topic_configs = config.get('topic_configs', None)
        self.__topic_configs = {}
        if topic_configs is not None:
            if isinstance(topic_configs, dict):
                self.__topic_configs = config['topic_configs']
            elif isinstance(topic_configs, str):
                config_entries = topic_configs.split(';')
                for config_entry in config_entries:
                    key, value = config_entry.split(':')
                    self.__topic_configs[key] = value
            elif isinstance(topic_configs, list):
                for item in topic_configs:
                    if isinstance(item, dict):
                        self.__topic_configs.update(item)
        if not self.__topic_configs:
            self.__topic_configs = None

    @property
    def bootstrap_servers(self):
        return self.__bootstrap_servers

    @property
    def topic(self):
        return self.__topic

    @property
    def partitions(self):
        return self.__partitions

    @property
    def replication_factor(self):
        return self.__replication_factor

    @property
    def replica_assignments(self):
        return self.__replica_assignments

    @property
    def topic_configs(self):
        return self.__topic_configs
