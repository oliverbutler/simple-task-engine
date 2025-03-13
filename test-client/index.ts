Bun.serve({
  port: 3000,
  routes: {
    "/task/:name": {
      POST: async (req) => {
        console.log(req.body);
        return Response.json({ ok: true });
      },
    },
  },
});

