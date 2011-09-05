namespace Volante.Impl
{
    using System;
    using System.Collections;
    using Volante;
    using System.Diagnostics;
    using Link = Volante.ILink<IPersistent>;

    class RtreePage : Persistent
    {
        const int card = (Page.pageSize - ObjectHeader.Sizeof - 4 * 3) / (4 * 4 + 4);
        const int minFill = card / 2;

        internal int n;
        internal Rectangle[] b;
        internal Link branch;

        internal RtreePage(IDatabase db, IPersistent obj, Rectangle r)
        {
            branch = db.CreateLink<IPersistent>(card);
            branch.Length = card;
            b = new Rectangle[card];
            setBranch(0, new Rectangle(r), obj);
            n = 1;
            for (int i = 1; i < card; i++)
            {
                b[i] = new Rectangle();
            }
        }

        internal RtreePage(IDatabase db, RtreePage root, RtreePage p)
        {
            branch = db.CreateLink<IPersistent>(card);
            branch.Length = card;
            b = new Rectangle[card];
            n = 2;
            setBranch(0, root.cover(), root);
            setBranch(1, p.cover(), p);
            for (int i = 2; i < card; i++)
            {
                b[i] = new Rectangle();
            }
        }

        internal RtreePage() { }

        internal RtreePage insert(IDatabase db, Rectangle r, IPersistent obj, int level)
        {
            Modify();
            if (--level != 0)
            {
                // not leaf page
                int i, mini = 0;
                long minIncr = long.MaxValue;
                long minArea = long.MaxValue;
                for (i = 0; i < n; i++)
                {
                    long area = b[i].Area();
                    long incr = Rectangle.JoinArea(b[i], r) - area;
                    if (incr < minIncr)
                    {
                        minIncr = incr;
                        minArea = area;
                        mini = i;
                    }
                    else if (incr == minIncr && area < minArea)
                    {
                        minArea = area;
                        mini = i;
                    }
                }
                RtreePage p = (RtreePage)branch[mini];
                RtreePage q = p.insert(db, r, obj, level);
                if (q == null)
                {
                    // child was not split
                    b[mini].Join(r);
                    return null;
                }
                else
                {
                    // child was split
                    setBranch(mini, p.cover(), p);
                    return addBranch(db, q.cover(), q);
                }
            }
            else
            {
                return addBranch(db, new Rectangle(r), obj);
            }
        }

        internal int remove(Rectangle r, IPersistent obj, int level, ArrayList reinsertList)
        {
            if (--level != 0)
            {
                for (int i = 0; i < n; i++)
                {
                    if (r.Intersects(b[i]))
                    {
                        RtreePage pg = (RtreePage)branch[i];
                        int reinsertLevel = pg.remove(r, obj, level, reinsertList);
                        if (reinsertLevel >= 0)
                        {
                            if (pg.n >= minFill)
                            {
                                setBranch(i, pg.cover(), pg);
                                Modify();
                            }
                            else
                            {
                                // not enough entries in child
                                reinsertList.Add(pg);
                                reinsertLevel = level - 1;
                                removeBranch(i);
                            }
                            return reinsertLevel;
                        }
                    }
                }
            }
            else
            {
                for (int i = 0; i < n; i++)
                {
                    if (branch.ContainsElement(i, obj))
                    {
                        removeBranch(i);
                        return 0;
                    }
                }
            }
            return -1;
        }

        internal void find(Rectangle r, ArrayList result, int level)
        {
            if (--level != 0)
            { /* this is an internal node in the tree */
                for (int i = 0; i < n; i++)
                {
                    if (r.Intersects(b[i]))
                    {
                        ((RtreePage)branch[i]).find(r, result, level);
                    }
                }
            }
            else
            { /* this is a leaf node */
                for (int i = 0; i < n; i++)
                {
                    if (r.Intersects(b[i]))
                    {
                        result.Add(branch[i]);
                    }
                }
            }
        }

        internal void purge(int level)
        {
            if (--level != 0)
            { /* this is an internal node in the tree */
                for (int i = 0; i < n; i++)
                {
                    ((RtreePage)branch[i]).purge(level);
                }
            }
            Deallocate();
        }

        void setBranch(int i, Rectangle r, IPersistent obj)
        {
            b[i] = r;
            branch[i] = obj;
        }

        void removeBranch(int i)
        {
            n -= 1;
            Array.Copy(b, i + 1, b, i, n - i);
            branch.RemoveAt(i);
            branch.Length = card;
            Modify();
        }

        RtreePage addBranch(IDatabase db, Rectangle r, IPersistent obj)
        {
            if (n < card)
            {
                setBranch(n++, r, obj);
                return null;
            }
            else
            {
                return splitPage(db, r, obj);
            }
        }

        RtreePage splitPage(IDatabase db, Rectangle r, IPersistent obj)
        {
            int i, j, seed0 = 0, seed1 = 0;
            long[] rectArea = new long[card + 1];
            long waste;
            long worstWaste = long.MinValue;
            //
            // As the seeds for the two groups, find two rectangles which waste 
            // the most area if covered by a single rectangle.
            //
            rectArea[0] = r.Area();
            for (i = 0; i < card; i++)
            {
                rectArea[i + 1] = b[i].Area();
            }
            Rectangle bp = r;
            for (i = 0; i < card; i++)
            {
                for (j = i + 1; j <= card; j++)
                {
                    waste = Rectangle.JoinArea(bp, b[j - 1]) - rectArea[i] - rectArea[j];
                    if (waste > worstWaste)
                    {
                        worstWaste = waste;
                        seed0 = i;
                        seed1 = j;
                    }
                }
                bp = b[i];
            }
            byte[] taken = new byte[card];
            Rectangle group0, group1;
            long groupArea0, groupArea1;
            int groupCard0, groupCard1;
            RtreePage pg;

            taken[seed1 - 1] = 2;
            group1 = new Rectangle(b[seed1 - 1]);

            if (seed0 == 0)
            {
                group0 = new Rectangle(r);
                pg = new RtreePage(db, obj, r);
            }
            else
            {
                group0 = new Rectangle(b[seed0 - 1]);
                pg = new RtreePage(db, branch.GetRaw(seed0 - 1), group0);
                setBranch(seed0 - 1, r, obj);
            }
            groupCard0 = groupCard1 = 1;
            groupArea0 = rectArea[seed0];
            groupArea1 = rectArea[seed1];
            //
            // Split remaining rectangles between two groups.
            // The one chosen is the one with the greatest difference in area 
            // expansion depending on which group - the rect most strongly 
            // attracted to one group and repelled from the other.
            //
            while (groupCard0 + groupCard1 < card + 1
                && groupCard0 < card + 1 - minFill
                && groupCard1 < card + 1 - minFill)
            {
                int betterGroup = -1, chosen = -1;
                long biggestDiff = -1;
                for (i = 0; i < card; i++)
                {
                    if (taken[i] == 0)
                    {
                        long diff = (Rectangle.JoinArea(group0, b[i]) - groupArea0)
                            - (Rectangle.JoinArea(group1, b[i]) - groupArea1);
                        if (diff > biggestDiff || -diff > biggestDiff)
                        {
                            chosen = i;
                            if (diff < 0)
                            {
                                betterGroup = 0;
                                biggestDiff = -diff;
                            }
                            else
                            {
                                betterGroup = 1;
                                biggestDiff = diff;
                            }
                        }
                    }
                }
                Debug.Assert(chosen >= 0);
                if (betterGroup == 0)
                {
                    group0.Join(b[chosen]);
                    groupArea0 = group0.Area();
                    taken[chosen] = 1;
                    pg.setBranch(groupCard0++, b[chosen], branch.GetRaw(chosen));
                }
                else
                {
                    groupCard1 += 1;
                    group1.Join(b[chosen]);
                    groupArea1 = group1.Area();
                    taken[chosen] = 2;
                }
            }
            //
            // If one group gets too full, then remaining rectangle are
            // split between two groups in such way to balance cards of two groups.
            //
            if (groupCard0 + groupCard1 < card + 1)
            {
                for (i = 0; i < card; i++)
                {
                    if (taken[i] == 0)
                    {
                        if (groupCard0 >= groupCard1)
                        {
                            taken[i] = 2;
                            groupCard1 += 1;
                        }
                        else
                        {
                            taken[i] = 1;
                            pg.setBranch(groupCard0++, b[i], branch.GetRaw(i));
                        }
                    }
                }
            }
            pg.n = groupCard0;
            n = groupCard1;
            for (i = 0, j = 0; i < groupCard1; j++)
            {
                if (taken[j] == 2)
                {
                    setBranch(i++, b[j], branch.GetRaw(j));
                }
            }
            return pg;
        }

        internal Rectangle cover()
        {
            Rectangle r = new Rectangle(b[0]);
            for (int i = 1; i < n; i++)
            {
                r.Join(b[i]);
            }
            return r;
        }
    }
}