import unittest

from livepeer_gateway.errors import LivepeerGatewayError
from livepeer_gateway.orchestrator import GetPayment
from livepeer_gateway import lp_rpc_pb2


class TestGetPaymentLV2V(unittest.TestCase):
    def test_requires_model_id_for_lv2v(self):
        info = lp_rpc_pb2.OrchestratorInfo()
        with self.assertRaises(LivepeerGatewayError):
            GetPayment("", info, typ="lv2v", model_id=None)

    def test_offchain_lv2v_with_model_id(self):
        info = lp_rpc_pb2.OrchestratorInfo()
        info.auth_token.token = b"token"
        info.auth_token.session_id = "session"

        resp = GetPayment("", info, typ="lv2v", model_id="modelA")
        self.assertEqual(resp.payment, "")
        self.assertIsInstance(resp.seg_creds, str)
        self.assertTrue(resp.seg_creds)


if __name__ == "__main__":
    unittest.main()
