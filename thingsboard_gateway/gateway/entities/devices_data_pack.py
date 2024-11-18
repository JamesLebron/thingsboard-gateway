# ------------------------------------------------------------------------------
#      Copyright 2024. ThingsBoard
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

from thingsboard_gateway.gateway.constants import TELEMETRY_VALUES_PARAMETER, TELEMETRY_TIMESTAMP_PARAMETER
from thingsboard_gateway.gateway.entities.converted_data import ConvertedData
from thingsboard_gateway.gateway.entities.datapoint_key import DatapointKey


class DevicesDataPack:
    def __init__(self):
        self._attributes_data = {}
        self._telemetry_data = {}
        self.__device_ts_index = {}
        self._attributes_count = 0
        self._telemetry_count = 0

    def append(self, data: ConvertedData):
        if data.attributes and data.device_name not in self._attributes_data:
            self._attributes_data[data.device_name] = {}
        if data.telemetry and data.device_name not in self._telemetry_data:
            self._telemetry_data[data.device_name] = []
            self.__device_ts_index[data.device_name] = {}

        if data.attributes:
            for datapoint_key, value in data.attributes.items():
                self._attributes_data[data.device_name][datapoint_key.key] = value
            self._attributes_count += data.attributes_datapoints_count

        if data.telemetry:
            for telemetry_entry in data.telemetry:
                if telemetry_entry.ts not in self.__device_ts_index[data.device_name]:
                    self.__device_ts_index[data.device_name][telemetry_entry] = len(self._telemetry_data[data.device_name])
                    initial_ts_metadata_values_object = {
                        TELEMETRY_TIMESTAMP_PARAMETER: telemetry_entry.ts,
                        TELEMETRY_VALUES_PARAMETER: {
                            key.key if isinstance(key, DatapointKey) else key : value for key, value in telemetry_entry.values.items()
                        }
                    }
                    # TODO: Check is metadata is correct, to avoid data loss
                    # if telemetry_entry.metadata:
                    #     initial_ts_metadata_values_object["metadata"] = telemetry_entry.metadata
                    self._telemetry_data[data.device_name].append(initial_ts_metadata_values_object)
                else:
                    existing_object = self._telemetry_data[data.device_name][self.__device_ts_index[data.device_name][telemetry_entry]]
                    existing_object[TELEMETRY_VALUES_PARAMETER].update({datapoint_key.key:value for datapoint_key, value in telemetry_entry.values.items()})
                    # if telemetry_entry.metadata:
                    #     existing_object["metadata"].update(telemetry_entry.metadata)
                    self._telemetry_data[data.device_name][self.__device_ts_index[data.device_name][telemetry_entry]] = existing_object
            self._telemetry_count += data.telemetry_datapoints_count

    def get_attributes_data(self):
        return self._attributes_data

    def get_telemetry_data(self):
        return self._telemetry_data

    def __len__(self):
        return self._attributes_count + self._telemetry_count