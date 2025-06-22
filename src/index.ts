import { Pool } from '@neondatabase/serverless';

export interface Env {
	DATABASE_URL: string;
}

// 辅助函数：将数据库查询结果（扁平）聚合成嵌套结构
function aggregateRoads(rows: any[]): any[] {
    if (!rows || rows.length === 0) {
        return [];
    }
    
    const roadMap = new Map<string, { road_name: string, streets: Set<string>, districts: Set<string> }>();

    for (const row of rows) {
        if (!roadMap.has(row.road_name)) {
            roadMap.set(row.road_name, {
                road_name: row.road_name,
                streets: new Set<string>(),
                districts: new Set<string>()
            });
        }
        const roadEntry = roadMap.get(row.road_name)!;
        if (row.street_name) roadEntry.streets.add(row.street_name);
        if (row.district_name) roadEntry.districts.add(row.district_name);
    }

    // 将 Set 转换为排序后的数组，以保证输出顺序稳定
    return Array.from(roadMap.values()).map(road => ({
        ...road,
        streets: Array.from(road.streets).sort(),
        districts: Array.from(road.districts).sort()
    }));
}


export default {
	async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
		const pool = new Pool({ connectionString: env.DATABASE_URL });
		const url = new URL(request.url);
		const { pathname, searchParams } = url;

		// --- API 路由 ---
		try {
			// --- 端点: GET /districts ---
			if (pathname === '/districts') {
				const { rows } = await pool.query(`SELECT "district_name" FROM districts ORDER BY "district_name"`);
				const districtList = rows.map(row => row.district_name);
				return new Response(JSON.stringify(districtList), { headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
			}

			// --- 端点: GET /streets ---
			if (pathname === '/streets') {
				let query = `SELECT "street_name", "district_name" FROM district_street_relations WHERE 1=1`;
				const params = [];
				if (searchParams.has('district')) {
					params.push(searchParams.get('district'));
					query += ` AND "district_name" = $${params.length}`;
				}
				if (searchParams.has('name')) {
					params.push(`%${searchParams.get('name')}%`);
					query += ` AND "street_name" LIKE $${params.length}`;
				}
				query += ` ORDER BY "district_name", "street_name"`;

				const { rows } = await pool.query(query, params);
				return new Response(JSON.stringify(rows), { headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
			}

			// --- 端点: GET /roads ---
			if (pathname === '/roads') {
                const page = parseInt(searchParams.get('page') || '1', 10);
                const pageSize = parseInt(searchParams.get('pageSize') || '20', 10);
                const offset = (page - 1) * pageSize;

				let baseQuery = `
                    FROM street_road_relations s_r
                    LEFT JOIN road_district_relations r_d ON s_r.road_name = r_d.road_name
                `;
				const whereClauses = [];
				const params: any[] = [];
                let paramIndex = 1;

				if (searchParams.has('district')) {
                    whereClauses.push(`r_d.district_name = $${paramIndex++}`);
					params.push(searchParams.get('district'));
				}
				if (searchParams.has('street')) {
					whereClauses.push(`s_r.street_name = $${paramIndex++}`);
					params.push(searchParams.get('street'));
				}
				if (searchParams.has('name')) {
					whereClauses.push(`s_r.road_name LIKE $${paramIndex++}`);
					params.push(`%${searchParams.get('name')}%`);
				}

                let whereClause = whereClauses.length > 0 ? `WHERE ${whereClauses.join(' AND ')}` : '';
                
                // 构建用于计数的查询
                const countQuery = `SELECT COUNT(DISTINCT s_r.road_name) ${baseQuery} ${whereClause}`;
                const totalResult = await pool.query(countQuery, params);
                const totalRecords = parseInt(totalResult.rows[0].count, 10);

                // 构建用于获取分页后 road_name 的查询
                const roadNameQuery = `
                    SELECT DISTINCT s_r.road_name 
                    ${baseQuery} ${whereClause}
                    ORDER BY s_r.road_name
                    LIMIT $${paramIndex++} OFFSET $${paramIndex++}
                `;
                const pagedRoadsResult = await pool.query(roadNameQuery, [...params, pageSize, offset]);
                const pagedRoadNames = pagedRoadsResult.rows.map(r => r.road_name);

                let finalData = [];
                if (pagedRoadNames.length > 0) {
                    // 获取这些分页后的道路的所有相关信息
                    const dataQuery = `
                        SELECT
                            s_r.road_name,
                            s_r.street_name,
                            r_d.district_name
                        FROM street_road_relations s_r
                        LEFT JOIN road_district_relations r_d ON s_r.road_name = r_d.road_name
                        WHERE s_r.road_name = ANY($1::text[])
                    `;
                    const { rows } = await pool.query(dataQuery, [pagedRoadNames]);
                    finalData = aggregateRoads(rows);
                }

                const response = {
                    meta: {
                        totalRecords,
                        page,
                        pageSize,
                        totalPages: Math.ceil(totalRecords / pageSize),
                    },
                    data: finalData
                };

				return new Response(JSON.stringify(response), { headers: { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' } });
			}

			// --- 404 Not Found ---
			return new Response(JSON.stringify({ error: 'Not Found' }), { status: 404, headers: { 'Content-Type': 'application/json' } });

		} catch (error: any) {
			console.error('An error occurred:', error);
			return new Response(JSON.stringify({ error: 'Internal Server Error', message: error.message }), { status: 500, headers: { 'Content-Type': 'application/json' } });
		} finally {
			ctx.waitUntil(pool.end());
		}
	},
};