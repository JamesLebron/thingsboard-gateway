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
        self.__bootstrap_servers = config.get('bootstrap_servers', ["localhost:9092"])
        self.__topic = config.get('topic', 'gateway.uplink')
        self.__partitions = config.get('partitions', 3)
        self.__replication_factor = config.get('replication_factor', 1)

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
