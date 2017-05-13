import redis
import trace_path
import json


class Analyzer:
    def __init__(self, analyzed_path, db_connection=redis.Redis()):
        self._data_source = db_connection
        self._analyzed_path = analyzed_path
        isinstance(self._analyzed_path, trace_path.TracePath)

    @staticmethod
    def _analyze_path_timestamps(path_timestamps):
        """
        Will generate report for a singular trace path  
        :param path_timestamps: list
        :return: all path duration
        """
        isinstance(path_timestamps, list)
        return path_timestamps[-1] - path_timestamps[0]

    def _retrieve_events(self, name, start_index, step_size):
        data = self._data_source.zrangebyscore(name=name, start=start_index, num=step_size,
                                               min=-float("inf"), max=float("inf"))
        return data

    def _analyze_trace(self, trace_id):
        # loop setup
        current_size = 0
        trace_event_list_size = self._data_source.zcount(name=trace_id, min=-float("inf"), max=float("inf"))
        step_size = 1000
        path_durations = []
        data_collection = []

        while current_size <= trace_event_list_size or len(data_collection) is not 0:
            data_collection += (self._retrieve_events(name=trace_id, start_index=current_size, step_size=step_size))
            current_size = current_size + step_size

            # traverse time ordered events list
            while len(data_collection) != 0:
                if len(self._analyzed_path) > len(data_collection):
                    data_collection += (self._retrieve_events(name=trace_id, start_index=current_size, step_size=step_size))
                    current_size = current_size + step_size

                timestamps = []
                top_index = 0
                try:
                    path_iter = self._analyzed_path.path_events()
                    event_id = path_iter.next()
                    while len(data_collection) != 0:  # Inner loop for finding path's events
                        event = data_collection[top_index]
                        event_json = json.loads(event)
                        if event_id == event_json["event_id"]:
                            timestamps.append(event_json["timestamp"])
                            data_collection.remove(event)
                            event_id = path_iter.next()
                        elif self._analyzed_path.ids_in_same_event(event_id, event_json["event_id"]):
                            # This means that the current path had stopped, so it's not relevant
                            break
                        else:
                            # This event is not apart of this path, but can be in another path. need to skip and look
                            # for the current id
                            top_index += 1
                            pass

                except StopIteration:  # Had gone throw all path's events
                    path_durations.append(self._analyze_path_timestamps(timestamps))
        if len(path_durations) != 0:
            print "For trace " + trace_id + "- Max time for path is " + str(max(path_durations)) +\
                  ", Avg time for path is " +\
                  str(sum(path_durations)/len(path_durations))
        else:
            print "There are none valid paths for " + trace_id

    def analyze(self):
        print self._data_source.keys()
        for trace_id in self._data_source.keys():
            self._analyze_trace(trace_id)


def main():
    path = trace_path.TracePath(
        trace_paths_definitions="/local/dev/performaceAnalyzer/demo_data_generator/test_routes_definition")
    Analyzer(path).analyze()

if __name__ == "__main__":
    main()
