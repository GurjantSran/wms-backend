// Print Agent polling routes — authenticated via API key (not JWT).
// The Print Agent on the laptop polls these endpoints to pick up print jobs
// submitted by mobile devices through the main inbound print-proxy routes.
import express, { Router, Request, Response, NextFunction } from 'express';
import * as inboundController from '../controllers/inbound.controller';

const router: Router = express.Router();

// Simple API key middleware for Print Agent
function requireAgentKey(req: Request, res: Response, next: NextFunction) {
    const key = req.headers['x-print-agent-key'] as string;
    const expected = process.env.PRINT_AGENT_KEY;
    if (!expected) {
        return res.status(500).json({ error: 'PRINT_AGENT_KEY not configured on server' });
    }
    if (!key || key !== expected) {
        return res.status(401).json({ error: 'Invalid or missing agent key' });
    }
    next();
}

router.use(requireAgentKey);

// Print Agent polls for pending jobs
router.get('/pending', inboundController.printQueuePending);

// Print Agent reports job completion/failure
router.post('/:id/complete', inboundController.printQueueComplete);

export default router;
