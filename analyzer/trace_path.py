import ConfigParser
import ast


class TracePath:
    def __init__(self, trace_paths_definitions):
        parser = ConfigParser.SafeConfigParser()
        parser.read(trace_paths_definitions)
        self._path = []
        for event in parser.sections():
            for event_id in ast.literal_eval(parser.get(event, "event_ids")):
                self._path.append((event, int(event_id)))

    def __len__(self):
        return len(self._path)

    def path_events(self):
        for event in self._path:
            yield event[1]

    def ids_in_same_event(self, id1, id2):
        """
        Will check if given ids are in the same logical unit in the given path.
        :param id1: int
        :param id2: int
        :return: Bool answer
        """
        id1_with_metadata = [item for item in self._path if id1 == item[1]]
        id2_with_metadata = [item for item in self._path if id2 == item[1]]
        if len(id1_with_metadata) is 0 or len(id2_with_metadata) is 0:
            print "Found unknown event: or " + str(id1) + " or " + str(id2)
            return False
        return id1_with_metadata.pop()[0] == id2_with_metadata.pop()[0]
