import pytest
import mock
import os
import itertools
import json

import target_redshift


class TestTargetRedshift(object):

    def setup_method(self):
        self.config = {}

    @mock.patch('target_redshift.flush_streams')
    @mock.patch('target_redshift.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(self, dbSync_mock,
                                                                                     flush_streams_mock):
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = '{"currently_syncing": null}'

        target_redshift.persist_lines(self.config, lines)

        flush_streams_mock.assert_called_once()

    @mock.patch('target_redshift.flush_records')
    @mock.patch('target_redshift.DbSync')
    def test_persist_lines_without_state_messages_flushes_mid_stream(self, dbSync_mock, flush_records_mock):
        """A tap that emits no STATE message must still be able to flush mid-stream.

        Regression test: flush_streams() used to dereference `state` directly in the
        filter_streams branch, so any run crossing batch_size_rows with state=None
        raised AttributeError: 'NoneType' object has no attribute 'get'.
        """
        self.config['batch_size_rows'] = 20

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            # Drop the STATE messages to emulate a tap that never emits one
            lines = [line for line in f.readlines() if json.loads(line)['type'] != 'STATE']

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.record_primary_key_string.side_effect = (str(i) for i in itertools.count())

        with mock.patch('target_redshift.emit_state') as emit_state_mock:
            target_redshift.persist_lines(self.config, lines)

        # 40 records at a batch size of 20 => two mid-stream flushes, none of which raise
        assert flush_records_mock.call_count == 2
        # With no state to bookmark, flushed_state stays None and nothing is emitted
        assert all(call.args[0] is None for call in emit_state_mock.call_args_list)

    @mock.patch('target_redshift.flush_records')
    @mock.patch('target_redshift.DbSync')
    def test_persist_lines_with_state_messages_still_bookmarks_flushed_stream(self, dbSync_mock,
                                                                              flush_records_mock):
        """Taps that do emit STATE keep the existing bookmark behaviour."""
        self.config['batch_size_rows'] = 20

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = dbSync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None
        instance.record_primary_key_string.side_effect = (str(i) for i in itertools.count())

        with mock.patch('target_redshift.emit_state') as emit_state_mock:
            target_redshift.persist_lines(self.config, lines)

        assert flush_records_mock.call_count == 2
        emitted = [call.args[0] for call in emit_state_mock.call_args_list if call.args[0] is not None]
        assert emitted, 'expected at least one non-empty state to be emitted'
        assert 'logical1-logical1_table2' in emitted[-1]['bookmarks']
